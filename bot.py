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
# Threads PER NOVEL (Keep this reasonable so we can run multiple novels)
THREADS_PER_NOVEL = 30 
# How many novels to download AT ONCE
SIMULTANEOUS_NOVELS = 5

DATA_DIR = "data"
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ERRORS_FILE = os.path.join(DATA_DIR, "errors.json")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        # Global executor for all novels. 
        # Size = (Novels * Threads) + buffer
        max_threads = THREADS_PER_NOVEL * SIMULTANEOUS_NOVELS + 10
        self.executor = ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="bot_worker")
        
        # Semaphore to limit simultaneous novels
        self.sem = asyncio.Semaphore(SIMULTANEOUS_NOVELS)
        
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
        with open(PROCESSED_FILE, 'w') as f: json.dump(list(self.processed), f, indent=2)

    def save_errors(self):
        with open(ERRORS_FILE, 'w') as f: json.dump(self.errors, f, indent=2)

    def save_error(self, url, error_msg):
        self.errors[url] = str(error_msg)
        self.save_errors()

    def start(self):
        if not TOKEN: return
        application = Application.builder().token(TOKEN).build()
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! {SIMULTANEOUS_NOVELS} novels x {THREADS_PER_NOVEL} threads")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"⚡ **High-Speed Bot** ⚡\nProcessed: {len(self.processed)}")

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
            
            await update.message.reply_text(f"🚀 **Starting Batch**\nQueued: {len(to_process)}\nSimultaneous: {SIMULTANEOUS_NOVELS}")

            # Create tasks for all novels
            tasks = [self.process_novel(url, update, context) for url in to_process]
            # run them (limited by Semaphore inside process_novel)
            await asyncio.gather(*tasks)

            await update.message.reply_text("✅ **Batch Complete**")
        except Exception as e: logger.error(f"File Error: {e}")
        finally:
            if os.path.exists(file_path): os.remove(file_path)

    async def process_novel(self, url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        async with self.sem: # Wait for a slot
            status_msg = await update.message.reply_text(f"⏳ **Queueing:** {url}")
            progress_queue = queue.Queue()
            loop = asyncio.get_running_loop()
            start_time = time.time()
            
            future = loop.run_in_executor(self.executor, self._scrape_logic, url, progress_queue)
            
            last_text = ""
            last_update = 0
            while not future.done():
                try:
                    text = progress_queue.get_nowait()
                    if text != last_text and (time.time() - last_update) > 5:
                        try: await status_msg.edit_text(text); last_text = text; last_update = time.time()
                        except: pass
                except queue.Empty: await asyncio.sleep(1)

            try:
                epub_path = await future
                duration = int(time.time() - start_time)
                if epub_path and os.path.exists(epub_path):
                    await status_msg.delete()
                    await update.message.reply_document(document=open(epub_path, 'rb'), caption=f"📕 {os.path.basename(epub_path)}\n⏱️ {duration}s")
                    self.save_success(url)
                    os.remove(epub_path)
                else: raise Exception("File generation failed")
            except Exception as e:
                logger.error(f"Fail: {url} -> {e}")
                await status_msg.edit_text(f"❌ Error: {e}")
                self.save_error(url, str(e))

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Fetching: {url}")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            # Ensure the crawler uses our Global Executor to avoid spinning up new pools constantly
            if app.crawler:
                app.crawler.init_executor(THREADS_PER_NOVEL) 

            if app.crawler.novel_cover:
                try:
                    headers = {"Referer": "https://www.fanmtl.com/", "User-Agent": "Mozilla/5.0"}
                    response = app.crawler.scraper.get(app.crawler.novel_cover, headers=headers, timeout=10)
                    if response.status_code == 200:
                        cover_path = os.path.abspath(os.path.join(app.output_path, 'cover.jpg'))
                        with open(cover_path, 'wb') as f: f.write(response.content)
                        app.book_cover = cover_path
                except: pass

            app.chapters = app.crawler.chapters[:]
            app.pack_by_volume = False
            app.output_formats = {'epub': True}
            
            # Reduced logging to speed up tight loops
            total = len(app.chapters)
            for i, _ in enumerate(app.start_download()):
                if i % 50 == 0: progress_queue.put(f"🚀 {int(app.progress)}% ({url.split('/')[-1]})")
            
            if not [c for c in app.chapters if c.body]: raise Exception("Zero content.")
            progress_queue.put("📦 Binding...")
            for fmt, f in app.bind_books(): return f
        except Exception as e: raise e
        finally: app.destroy()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()
