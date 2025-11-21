import os
import logging
import asyncio
import queue
import time
import urllib3
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
import pymongo

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")  # Connection String from Northflank
THREADS_PER_NOVEL = 50
DATA_DIR = "data"
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        
        # --- MongoDB Connection ---
        if not MONGO_URL:
            logger.error("❌ MONGO_URL environment variable is missing!")
            exit(1)
            
        try:
            self.client = pymongo.MongoClient(MONGO_URL)
            self.db = self.client.get_database("lncrawl_bot")
            
            # Collections
            self.col_history = self.db.history      # Successfully processed
            self.col_queue = self.db.queue          # Pending jobs
            self.col_errors = self.db.errors        # Error logs
            
            # Create indexes for speed
            self.col_history.create_index("url", unique=True)
            self.col_queue.create_index([("status", 1), ("created_at", 1)])
            
            logger.info("✅ Connected to MongoDB")
        except Exception as e:
            logger.error(f"❌ MongoDB Connection Failed: {e}")
            exit(1)

    # --- DB HELPERS ---
    def is_processed(self, url):
        return self.col_history.find_one({"url": url}) is not None

    def add_to_history(self, url, chat_id, file_name):
        self.col_history.update_one(
            {"url": url},
            {"$set": {
                "processed_at": datetime.utcnow(),
                "chat_id": chat_id,
                "file": file_name
            }},
            upsert=True
        )

    def log_error(self, url, error_msg):
        self.col_errors.insert_one({
            "url": url,
            "error": str(error_msg),
            "timestamp": datetime.utcnow()
        })

    def get_pending_queue(self):
        """Fetch all pending jobs sorted by creation time"""
        return list(self.col_queue.find({"status": "pending"}).sort("created_at", 1))

    def add_queue_items(self, chat_id, urls):
        docs = []
        for url in urls:
            # Skip if already queued or processed
            if self.is_processed(url): continue
            if self.col_queue.find_one({"url": url, "status": "pending"}): continue
            
            docs.append({
                "chat_id": chat_id,
                "url": url,
                "status": "pending",
                "created_at": datetime.utcnow()
            })
        
        if docs:
            self.col_queue.insert_many(docs)
        return len(docs)

    def mark_queue_done(self, url):
        self.col_queue.delete_many({"url": url})

    # --- BOT LOGIC ---
    async def post_init(self, application: Application):
        """Checks for pending MongoDB queue items on boot."""
        pending = self.get_pending_queue()
        if pending:
            # Group by chat_id to notify users
            chats = set(item['chat_id'] for item in pending)
            for chat_id in chats:
                try:
                    count = sum(1 for i in pending if i['chat_id'] == chat_id)
                    await application.bot.send_message(
                        chat_id, 
                        f"🔄 **Bot Restarted**\nFound {count} pending novels in database.\nResuming..."
                    )
                except: pass
            
            # Resume processing
            asyncio.create_task(self.process_global_queue(application.bot))

    def start(self):
        if not TOKEN: 
            print("❌ TELEGRAM_TOKEN missing")
            return

        application = Application.builder().token(TOKEN).post_init(self.post_init).build()
        
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! Threads: {THREADS_PER_NOVEL}")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        count = self.col_history.count_documents({})
        await update.message.reply_text(f"⚡ **FanMTL Bot (MongoDB Edition)** ⚡\nProcessed in History: {count}")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Only clears queue, optionally clear history if really needed
        deleted = self.col_queue.delete_many({})
        await update.message.reply_text(f"🗑️ Queue cleared. Removed {deleted.deleted_count} pending items.")

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        temp_path = os.path.join(DATA_DIR, "temp_input.json")
        await file.download_to_drive(temp_path)
        
        try:
            import json
            with open(temp_path, 'r', encoding='utf-8') as f: urls = json.load(f)
            
            added_count = self.add_queue_items(update.effective_chat.id, urls)
            
            await update.message.reply_text(f"📥 **Imported:** {added_count} new novels to queue.")
            
            # Trigger processing
            asyncio.create_task(self.process_global_queue(context.bot))

        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ File Error: Invalid JSON")
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    async def process_global_queue(self, bot):
        """Reads from MongoDB and processes items one by one."""
        while True:
            # Fetch one pending item
            item = self.col_queue.find_one_and_update(
                {"status": "pending"},
                {"$set": {"status": "processing"}},
                sort=[("created_at", 1)]
            )
            
            if not item:
                break # Queue empty

            chat_id = item['chat_id']
            url = item['url']
            
            try:
                await self.process_novel(url, chat_id, bot)
            except Exception as e:
                logger.error(f"Queue Worker Error: {e}")
            finally:
                # Remove from queue regardless of success/fail (History/Errors handle the record)
                self.mark_queue_done(url)
                gc.collect()

    async def process_novel(self, url: str, chat_id: int, bot):
        # Final check against history to avoid duplicate work
        if self.is_processed(url):
            await bot.send_message(chat_id, f"⏩ **Already Processed:** {url}")
            return

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
                file_name = os.path.basename(epub_path)
                
                try: await status_msg.delete()
                except: pass

                await bot.send_document(
                    chat_id=chat_id,
                    document=open(epub_path, 'rb'),
                    caption=f"📕 {file_name}\n📦 {file_size:.1f}MB | ⏱️ {duration}s"
                )
                
                self.add_to_history(url, chat_id, file_name)
                os.remove(epub_path)
            else: 
                await status_msg.edit_text(f"❌ Failed to generate file for {url}")
                self.log_error(url, "No file generated")
        except Exception as e:
            logger.error(f"Fail: {url} -> {e}")
            try: await status_msg.edit_text(f"❌ Error: {e}")
            except: await bot.send_message(chat_id, f"❌ Error: {e}")
            self.log_error(url, str(e))

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Getting info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            if app.crawler: app.crawler.init_executor(THREADS_PER_NOVEL)

            # Cover Logic (Same as before)
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
            
            # --- INTEGRITY REPAIR ---
            failed = [c for c in app.chapters if not c.body or len(c.body.strip()) < 20]
            if failed:
                progress_queue.put(f"⚠️ Missing {len(failed)} chapters. Retrying...")
                app.crawler.download_chapters(failed)
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