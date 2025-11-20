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

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKEN = os.getenv("TELEGRAM_TOKEN")
# 100 Threads per novel = ~15-25 chapters/sec on good hardware
THREADS_PER_NOVEL = 100

DATA_DIR = "data"
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        self.load_history()

    def load_history(self):
        self.processed = set()
        if os.path.exists(PROCESSED_FILE):
            try:
                with open(PROCESSED_FILE, 'r') as f: self.processed = set(json.load(f))
            except: pass

    def save_success(self, url):
        self.processed.add(url)
        with open(PROCESSED_FILE, 'w') as f: json.dump(list(self.processed), f, indent=2)

    def start(self):
        if not TOKEN: return
        application = Application.builder().token(TOKEN).build()
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! Threads: {THREADS_PER_NOVEL}")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"⚡ **FanMTL Turbo** ⚡\nProcessed: {len(self.processed)}\nSend JSON to start.")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.processed = set()
        if os.path.exists(PROCESSED_FILE): os.remove(PROCESSED_FILE)
        await update.message.reply_text("🗑️ History Reset.")

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        file_path = os.path.join(DATA_DIR, f"input_{document.file_id}.json")
        await file.download_to_drive(file_path)
        try:
            with open(file_path, 'r', encoding='utf-8') as f: urls = json.load(f)
            to_process = [u for u in urls if u not in self.processed]
            await update.message.reply_text(f"📥 **Batch Started**\nNovels: {len(to_process)}\nMode: Sequential (Max Speed)")
            
            for url in to_process:
                await self.process_novel(url, update, context)
            
            await update.message.reply_text("✅ **Batch Complete**")
        except Exception as e: logger.error(f"File Error: {e}")
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
                # Limit updates to every 3s to avoid flooding Telegram
                if text != last_text and (time.time() - last_update) > 3:
                    try: await status_msg.edit_text(text); last_text = text; last_update = time.time()
                    except: pass
            except queue.Empty: await asyncio.sleep(0.5)

        try:
            epub_path = await future
            duration = int(time.time() - start_time)
            if epub_path and os.path.exists(epub_path):
                file_size = os.path.getsize(epub_path) / (1024 * 1024)
                await status_msg.edit_text(f"✅ **Done** ({duration}s) - {file_size:.1f}MB\nUploading...")
                await update.message.reply_document(
                    document=open(epub_path, 'rb'),
                    caption=f"📕 {os.path.basename(epub_path)}\n⏱️ {duration}s"
                )
                await status_msg.delete()
                self.save_success(url)
                os.remove(epub_path)
            else: raise Exception("File generation failed.")
        except Exception as e:
            logger.error(f"Fail: {url} -> {e}")
            await status_msg.edit_text(f"❌ Error: {e}")

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Getting info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            # Set High Thread Count
            if app.crawler: app.crawler.init_executor(THREADS_PER_NOVEL)

            # Download Cover (With Headers)
            if app.crawler.novel_cover:
                try:
                    headers = {"Referer": "https://www.fanmtl.com/", "User-Agent": "Mozilla/5.0"}
                    response = app.crawler.scraper.get(app.crawler.novel_cover, headers=headers, timeout=15)
                    if response.status_code == 200:
                        cover_path = os.path.abspath(os.path.join(app.output_path, 'cover.jpg'))
                        with open(cover_path, 'wb') as f: f.write(response.content)
                        app.book_cover = cover_path
                except: pass

            app.chapters = app.crawler.chapters[:]
            app.pack_by_volume = False
            app.output_formats = {'epub': True}
            
            total = len(app.chapters)
            progress_queue.put(f"⬇️ Downloading {total} chapters...")
            
            for i, _ in enumerate(app.start_download()):
                if i % 30 == 0: progress_queue.put(f"🚀 {int(app.progress)}% ({i}/{total})")
            
            # Safety: Ensure content exists
            valid_chapters = [c for c in app.chapters if c.body and len(c.body) > 50]
            if not valid_chapters: raise Exception("Critical: No content downloaded.")

            progress_queue.put(f"📦 Binding {len(valid_chapters)} chapters...")
            for fmt, f in app.bind_books(): return f
            return None

        except Exception as e: raise e
        finally: app.destroy()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()
