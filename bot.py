import os
import json
import logging
import asyncio
import shutil
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Import lncrawl components
from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
# AGGRESSIVE SPEED: 80 threads should hit ~15-20 chapters/sec on a good VPS
MAX_WORKERS = 80 
DOWNLOAD_DIR = "downloads"
PROCESSED_FILE = "processed.json"
ERRORS_FILE = "errors.json"

# Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        # Executor for the heavy lifting (Scraping)
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")
        self.load_history()

    def load_history(self):
        """Load processed and error lists from JSON files."""
        self.processed = set()
        self.errors = {}

        if os.path.exists(PROCESSED_FILE):
            try:
                with open(PROCESSED_FILE, 'r') as f:
                    self.processed = set(json.load(f))
            except Exception:
                logger.error("Failed to load processed.json")

        if os.path.exists(ERRORS_FILE):
            try:
                with open(ERRORS_FILE, 'r') as f:
                    self.errors = json.load(f)
            except Exception:
                logger.error("Failed to load errors.json")

    def save_success(self, url):
        """Save a URL to the processed list."""
        self.processed.add(url)
        # Remove from errors if it was there previously
        if url in self.errors:
            del self.errors[url]
            self.save_errors() # Update error file immediately
        
        with open(PROCESSED_FILE, 'w') as f:
            json.dump(list(self.processed), f, indent=2)

    def save_error(self, url, error_msg):
        """Save a URL and error message to the error list."""
        self.errors[url] = str(error_msg)
        with open(ERRORS_FILE, 'w') as f:
            json.dump(self.errors, f, indent=2)

    def start(self):
        if not TOKEN:
            print("Error: TELEGRAM_TOKEN environment variable not set.")
            return

        application = Application.builder().token(TOKEN).build()
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("🚀 Loading sources and warming up...")
        load_sources()
        print(f"✅ Bot online! Speed set to {MAX_WORKERS} threads.")
        
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            f"⚡ **High Performance Bot Online** ⚡\n\n"
            f"threads: `{MAX_WORKERS}`\n"
            f"Processed novels: `{len(self.processed)}`\n"
            f"Errored novels: `{len(self.errors)}`\n\n"
            "Upload a **JSON file** to start batch downloading."
        )

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        file_path = os.path.join(DOWNLOAD_DIR, f"{document.file_id}.json")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        await file.download_to_drive(file_path)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                urls = json.load(f)
            
            if not isinstance(urls, list):
                await update.message.reply_text("Error: JSON must contain a list of URLs.")
                return

            # Filter URLs
            to_process = [u for u in urls if u not in self.processed]
            skipped_count = len(urls) - len(to_process)

            msg = f"📥 **Batch Received**\nTotal: {len(urls)}\nSkipping: {skipped_count} (already done)\nQueueing: {len(to_process)}"
            await update.message.reply_text(msg)

            # Process sequentially to manage resources better, or use Semaphore for limited concurrency
            for url in to_process:
                await self.process_novel(url, update, context)

            await update.message.reply_text("✅ **Batch Run Complete**")

        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ Critical error processing file.")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    async def process_novel(self, url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_msg = await update.message.reply_text(f"⏳ **Starting:** {url}")
        
        progress_queue = queue.Queue()
        loop = asyncio.get_running_loop()
        
        start_time = time.time()
        
        # Run blocking scrape in background thread
        future = loop.run_in_executor(self.executor, self._scrape_logic, url, progress_queue)
        
        last_text = ""
        last_update = 0

        while not future.done():
            try:
                # Non-blocking queue get
                text = progress_queue.get_nowait()
                now = time.time()
                
                # Update Telegram message every 3 seconds max to avoid flood limits
                if text != last_text and (now - last_update) > 3:
                    try:
                        await status_msg.edit_text(text)
                        last_text = text
                        last_update = now
                    except Exception:
                        pass
            except queue.Empty:
                await asyncio.sleep(0.5)

        try:
            epub_path = await future
            duration = int(time.time() - start_time)
            
            if epub_path and os.path.exists(epub_path):
                file_size = os.path.getsize(epub_path) / (1024 * 1024) # MB
                await status_msg.edit_text(f"✅ **Success** ({duration}s)\nUploading {file_size:.1f}MB...")
                
                await update.message.reply_document(
                    document=open(epub_path, 'rb'),
                    caption=f"📕 {os.path.basename(epub_path)}\n⏱️ Time: {duration}s"
                )
                await status_msg.delete()
                
                self.save_success(url) # Save to history
                os.remove(epub_path)
            else:
                raise Exception("File was not generated.")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Fail: {url} -> {error_msg}")
            await status_msg.edit_text(f"❌ **Failed:** {url}\nReason: {error_msg}")
            self.save_error(url, error_msg) # Save to error log

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Fetching info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            # --- SPEED HACK START ---
            if app.crawler:
                # Override the crawler's executor with our high worker count
                app.crawler.init_executor(MAX_WORKERS)
            # --- SPEED HACK END ---

            app.chapters = app.crawler.chapters[:]
            app.pack_by_volume = False
            app.output_formats = {'epub': True}
            
            total_chapters = len(app.chapters)
            progress_queue.put(f"⬇️ Downloading {total_chapters} chapters (Threads: {MAX_WORKERS})...")

            # Download Loop
            for i, _ in enumerate(app.start_download()):
                # Only update queue occasionally to reduce overhead
                if i % 20 == 0 or i == total_chapters:
                    percent = int(app.progress)
                    # Calculate Speed roughly
                    progress_queue.put(f"🚀 Downloading: {percent}% ({i}/{total_chapters})")
            
            # --- FIX FOR INDEX ERROR ---
            # Check if we actually got chapters
            successful_chapters = [c for c in app.chapters if c.body]
            if not successful_chapters:
                raise Exception("No content was downloaded (0 successful chapters).")
            # ---------------------------

            progress_queue.put("📦 Compiling EPUB...")
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
