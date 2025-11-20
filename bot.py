import os
import json
import logging
import asyncio
import shutil
import queue
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

# Disable SSL warnings at high concurrency
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
MAX_WORKERS = 200 
DATA_DIR = "data"
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ERRORS_FILE = os.path.join(DATA_DIR, "errors.json")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        self.load_history()

    def load_history(self):
        self.processed = set()
        self.errors = {}
        if os.path.exists(PROCESSED_FILE):
            try:
                with open(PROCESSED_FILE, 'r') as f: self.processed = set(json.load(f))
            except Exception: pass
        if os.path.exists(ERRORS_FILE):
            try:
                with open(ERRORS_FILE, 'r') as f: self.errors = json.load(f)
            except Exception: pass

    def save_success(self, url):
        self.processed.add(url)
        if url in self.errors:
            del self.errors[url]
            self.save_errors()
        with open(PROCESSED_FILE, 'w') as f:
            json.dump(list(self.processed), f, indent=2)

    def save_errors(self):
        with open(ERRORS_FILE, 'w') as f:
            json.dump(self.errors, f, indent=2)

    def save_error(self, url, error_msg):
        self.errors[url] = str(error_msg)
        self.save_errors()

    def start(self):
        if not TOKEN:
            print("Error: TELEGRAM_TOKEN not set.")
            return

        application = Application.builder().token(TOKEN).build()
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! (Threads: {MAX_WORKERS})")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            f"⚡ **FanMTL Bot** ⚡\n"
            f"Processed: `{len(self.processed)}`\n"
            "Send a JSON file to start."
        )

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.processed = set()
        self.errors = {}
        if os.path.exists(PROCESSED_FILE): os.remove(PROCESSED_FILE)
        if os.path.exists(ERRORS_FILE): os.remove(ERRORS_FILE)
        await update.message.reply_text("🗑️ History Reset. Re-send your JSON file.")

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        file_path = os.path.join(DATA_DIR, f"input_{document.file_id}.json")
        await file.download_to_drive(file_path)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                urls = json.load(f)
            
            to_process = [u for u in urls if u not in self.processed]
            await update.message.reply_text(f"📥 **Batch Received**\nQueueing: {len(to_process)} novels")

            for url in to_process:
                await self.process_novel(url, update, context)

            await update.message.reply_text("✅ **Batch Complete**")

        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ File error.")
        finally:
            if os.path.exists(file_path): os.remove(file_path)

    async def process_novel(self, url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_msg = await update.message.reply_text(f"⏳ **Starting:** {url}")
        progress_queue = queue.Queue()
        loop = asyncio.get_running_loop()
        start_time = time.time()
        
        future = loop.run_in_executor(self.executor, self._scrape_logic, url, progress_queue)
        
        last_text = ""
        last_update = 0

        while not future.done():
            try:
                text = progress_queue.get_nowait()
                now = time.time()
                if text != last_text and (now - last_update) > 2:
                    try:
                        await status_msg.edit_text(text)
                        last_text = text
                        last_update = now
                    except Exception: pass
            except queue.Empty:
                await asyncio.sleep(0.5)

        try:
            epub_path = await future
            duration = int(time.time() - start_time)
            
            if epub_path and os.path.exists(epub_path):
                file_size = os.path.getsize(epub_path) / (1024 * 1024)
                await status_msg.edit_text(f"✅ **Success** ({duration}s)\nUploading {file_size:.1f}MB...")
                
                await update.message.reply_document(
                    document=open(epub_path, 'rb'),
                    caption=f"📕 {os.path.basename(epub_path)}\n⏱️ {duration}s"
                )
                await status_msg.delete()
                self.save_success(url)
                os.remove(epub_path)
            else:
                raise Exception("File not generated.")
                
        except Exception as e:
            logger.error(f"Fail: {url} -> {e}", exc_info=True)
            await status_msg.edit_text(f"❌ Error: {str(e)}")
            self.save_error(url, str(e))

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Fetching info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            if app.crawler:
                app.crawler.init_executor(MAX_WORKERS)

            # --- COVER DOWNLOAD ---
            if app.crawler.novel_cover:
                try:
                    progress_queue.put("🖼️ Downloading cover...")
                    # Headers are critical for images protected by Cloudflare/Hotlink rules
                    headers = {
                        "Referer": "https://www.fanmtl.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    }
                    # Use the internal scraper to perform the GET request
                    response = app.crawler.scraper.get(app.crawler.novel_cover, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        cover_path = os.path.abspath(os.path.join(app.output_path, 'cover.jpg'))
                        with open(cover_path, 'wb') as f:
                            f.write(response.content)
                        app.book_cover = cover_path
                except Exception as e:
                    logger.warning(f"Cover error: {e}")

            app.chapters = app.crawler.chapters[:]
            app.pack_by_volume = False
            app.output_formats = {'epub': True}
            
            total = len(app.chapters)
            progress_queue.put(f"⬇️ Downloading {total} chapters...")

            for i, _ in enumerate(app.start_download()):
                if i % 40 == 0 or i == total:
                    percent = int(app.progress)
                    progress_queue.put(f"🚀 Downloading: {percent}% ({i}/{total})")
            
            if not [c for c in app.chapters if c.body]:
                raise Exception("Zero chapters downloaded.")

            progress_queue.put("📦 Binding...")
            generated = [f for fmt, f in app.bind_books()]
            
            if generated:
                final = os.path.join(DOWNLOAD_DIR, os.path.basename(generated[0]))
                shutil.copy(generated[0], final)
                return final
            return None
        except Exception as e:
            raise e
        finally:
            app.destroy()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()
