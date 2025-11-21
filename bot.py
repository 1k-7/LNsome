import os
import json
import logging
import asyncio
import queue
import time
import urllib3
import gc
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

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
THREADS_PER_NOVEL = 50 # Safe high speed for single novel
DATA_DIR = "data"
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ERRORS_FILE = os.path.join(DATA_DIR, "errors.json")
# The persistent queue file
QUEUE_FILE = os.path.join(DATA_DIR, "queue.json")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
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
            except: pass
        if os.path.exists(ERRORS_FILE):
            try:
                with open(ERRORS_FILE, 'r') as f: self.errors = json.load(f)
            except: pass

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

    # --- AUTO RESUME ON STARTUP ---
    async def post_init(self, application: Application):
        """Checks for an unfinished queue file on boot."""
        if os.path.exists(QUEUE_FILE):
            try:
                logger.info("📂 Found valid queue file. Resuming...")
                with open(QUEUE_FILE, 'r') as f:
                    data = json.load(f)
                
                if "chat_id" in data and "urls" in data:
                    chat_id = data["chat_id"]
                    urls = data["urls"]
                    
                    # Calculate what's left
                    pending = [u for u in urls if u not in self.processed]
                    
                    if pending:
                        await application.bot.send_message(
                            chat_id, 
                            f"🔄 **Bot Restarted**\nFound saved queue.\nResuming {len(pending)} novels..."
                        )
                        # Start processing in background
                        asyncio.create_task(self.process_queue(chat_id, urls, application.bot))
                    else:
                        logger.info("Queue exists but all novels processed. Deleting.")
                        os.remove(QUEUE_FILE)
            except Exception as e:
                logger.error(f"Auto-resume failed: {e}")

    def start(self):
        if not TOKEN: return
        # Hook post_init to run resume logic
        application = Application.builder().token(TOKEN).post_init(self.post_init).build()
        
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! Threads: {THREADS_PER_NOVEL}")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"⚡ **FanMTL Bot** ⚡\nProcessed: {len(self.processed)}")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.processed = set()
        if os.path.exists(PROCESSED_FILE): os.remove(PROCESSED_FILE)
        if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
        await update.message.reply_text("🗑️ History & Queue Reset.")

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        temp_path = os.path.join(DATA_DIR, "temp_input.json")
        await file.download_to_drive(temp_path)
        
        try:
            with open(temp_path, 'r', encoding='utf-8') as f: urls = json.load(f)
            
            # --- SAVE PERSISTENT QUEUE ---
            queue_data = {
                "chat_id": update.effective_chat.id,
                "urls": urls
            }
            with open(QUEUE_FILE, 'w') as f:
                json.dump(queue_data, f, indent=2)
            # -----------------------------

            await self.process_queue(update.effective_chat.id, urls, context.bot)

        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ File Error")
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    async def process_queue(self, chat_id, urls, bot):
        """Iterates through the queue sequentially."""
        to_process = [u for u in urls if u not in self.processed]
        
        if not to_process:
            await bot.send_message(chat_id, "✅ **Queue Complete**")
            if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
            return

        await bot.send_message(chat_id, f"📥 **Queue Started**\nPending: {len(to_process)}")
        
        for url in to_process:
            # Double check in case of race condition
            if url in self.processed: continue
            
            await self.process_novel(url, chat_id, bot)
            gc.collect() # Free memory
        
        # If we reach here, queue is done
        await bot.send_message(chat_id, "✅ **All Tasks Finished**")
        if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)

    async def process_novel(self, url: str, chat_id: int, bot):
        status_msg = await bot.send_message(chat_id, f"⏳ **Starting:** {url}")
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
                    try: 
                        await status_msg.edit_text(text)
                        last_text = text
                        last_update = time.time()
                    except: pass
            except queue.Empty: await asyncio.sleep(0.5)

        try:
            epub_path = await future
            duration = int(time.time() - start_time)
            
            if epub_path and os.path.exists(epub_path):
                file_size = os.path.getsize(epub_path) / (1024 * 1024)
                
                # Clean up status message
                try: await status_msg.delete()
                except: pass

                await bot.send_document(
                    chat_id=chat_id,
                    document=open(epub_path, 'rb'),
                    caption=f"📕 {os.path.basename(epub_path)}\n📦 {file_size:.1f}MB | ⏱️ {duration}s"
                )
                
                self.save_success(url)
                os.remove(epub_path)
            else: 
                await status_msg.edit_text(f"❌ Failed to generate file for {url}")
        except Exception as e:
            logger.error(f"Fail: {url} -> {e}")
            try: await status_msg.edit_text(f"❌ Error: {e}")
            except: await bot.send_message(chat_id, f"❌ Error: {e}")
            self.save_error(url, str(e))

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Getting info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            if app.crawler: app.crawler.init_executor(THREADS_PER_NOVEL)

            # Cover
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
                if i % 40 == 0: 
                    progress_queue.put(f"🚀 {int(app.progress)}% ({i}/{total})")
            
            # --- INTEGRITY REPAIR & SEND ANYWAY ---
            failed = [c for c in app.chapters if not c.body or len(c.body.strip()) < 20]
            if failed:
                progress_queue.put(f"⚠️ Missing {len(failed)} chapters. Retrying...")
                # Retry once
                app.crawler.download_chapters(failed)
                # Final Check
                failed = [c for c in app.chapters if not c.body or len(c.body.strip()) < 20]
                if failed:
                     progress_queue.put(f"⚠️ Sending incomplete file (Missing {len(failed)}).")
                     for c in failed:
                         c.body = f"<h1>Chapter {c.id}</h1><p><i>[Chapter Content Missing from Source]</i></p>"

            progress_queue.put("📦 Binding...")
            for fmt, f in app.bind_books(): return f
            return None

        except Exception as e: raise e
        finally: 
            app.destroy()
            gc.collect()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()
