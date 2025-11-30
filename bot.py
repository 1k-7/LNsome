import os
import json
import logging
import asyncio
import shutil
import queue
import time
import urllib3
import gc
import uuid
import zipfile
import datetime
from concurrent.futures import ThreadPoolExecutor

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from pyrogram import Client as UserBotClient

from lncrawl.core.app import App
from lncrawl.core.sources import load_sources

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
# REDUCED: 50 threads per novel is overkill and eats RAM. 
# 8 is the sweet spot for speed vs memory.
THREADS_PER_NOVEL = 8

# Group Configs (Must be -100xxxx format)
TARGET_GROUP_ID = os.getenv("TARGET_GROUP_ID") 
ERROR_GROUP_ID = os.getenv("ERROR_GROUP_ID")   

# --- MANUAL OVERRIDES (Failsafe) ---
FORCE_TARGET_TOPIC_ID = os.getenv("FORCE_TARGET_TOPIC_ID")
FORCE_ERROR_TOPIC_ID = os.getenv("FORCE_ERROR_TOPIC_ID")

# Userbot Config
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
USERBOT_THRESHOLD = 40.0 

DATA_DIR = os.getenv("DATA_DIR", "data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")

# Ensure Data Directory Exists
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

pending_uploads = {}

class NovelBot:
    def __init__(self):
        # REDUCED: Only process 2 novels at once. 
        # Processing 5 concurrent heavy crawls is what spikes usage to GBs.
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="bot_worker")
        
        self.userbot = None
        self.bot_username = None 
        
        # State
        self.processed = set()
        self.errors = {}
        
        # Topic IDs
        self.target_topic_id = int(FORCE_TARGET_TOPIC_ID) if FORCE_TARGET_TOPIC_ID else None
        self.error_topic_id = int(FORCE_ERROR_TOPIC_ID) if FORCE_ERROR_TOPIC_ID else None
        self.backup_topic_id = None

        # File Paths (Set in post_init)
        self.files = {}

    def get_file_path(self, name):
        """Generates a namespaced file path: data/name_BotUsername.json"""
        return os.path.join(DATA_DIR, f"{name}_{self.bot_username}.json")

    def load_data(self):
        """Loads data with verbose error logging and migration support"""
        
        # --- 1. Load Processed Novels ---
        if os.path.exists(self.files['processed']):
            try:
                with open(self.files['processed'], 'r') as f: self.processed = set(json.load(f))
            except Exception as e: logger.error(f"⚠️ Load Processed Failed: {e}")
        # Legacy check
        elif os.path.exists(os.path.join(DATA_DIR, "processed.json")):
            try:
                with open(os.path.join(DATA_DIR, "processed.json"), 'r') as f: 
                    self.processed = set(json.load(f))
                logger.info("♻️ Migrated processed.json")
                # We don't save immediately to avoid race conditions, will save on next write
            except Exception as e: logger.error(f"⚠️ Legacy Processed Load Failed: {e}")

        # --- 2. Load Errors ---
        if os.path.exists(self.files['errors']):
            try:
                with open(self.files['errors'], 'r') as f: self.errors = json.load(f)
            except Exception as e: logger.error(f"⚠️ Load Errors Failed: {e}")

        # --- 3. Load Topics (Critical) ---
        if not self.target_topic_id or not self.error_topic_id:
            loaded = False
            
            # A. Check New Specific File
            if os.path.exists(self.files['topics']):
                try:
                    with open(self.files['topics'], 'r') as f:
                        data = json.load(f)
                        if not self.target_topic_id: self.target_topic_id = data.get("target_topic_id")
                        if not self.error_topic_id: self.error_topic_id = data.get("error_topic_id")
                        self.backup_topic_id = data.get("backup_topic_id")
                        loaded = True
                        logger.info(f"✅ Loaded Topics from {os.path.basename(self.files['topics'])}")
                except Exception as e: 
                    logger.error(f"❌ CRITICAL: Found {self.files['topics']} but could not read it: {e}")

            # B. Check Legacy File (Migration)
            if not loaded:
                legacy_path = os.path.join(DATA_DIR, "topics.json")
                if os.path.exists(legacy_path):
                    try:
                        with open(legacy_path, 'r') as f:
                            data = json.load(f)
                            if not self.target_topic_id: self.target_topic_id = data.get("target_topic_id")
                            if not self.error_topic_id: self.error_topic_id = data.get("error_topic_id")
                            self.backup_topic_id = data.get("backup_topic_id")
                            logger.info(f"♻️ Migrated Topics from legacy topics.json")
                    except Exception as e: 
                        logger.error(f"❌ CRITICAL: Found legacy topics.json but could not read it: {e}")

    def save_topics(self):
        """Saves current topic IDs to namespaced file"""
        try:
            with open(self.files['topics'], 'w') as f:
                json.dump({
                    "target_topic_id": self.target_topic_id,
                    "error_topic_id": self.error_topic_id,
                    "backup_topic_id": self.backup_topic_id
                }, f, indent=2)
            logger.info(f"💾 Topics Saved to {os.path.basename(self.files['topics'])}")
        except Exception as e:
            logger.error(f"❌ Could not save topics: {e}")

    def save_success(self, url):
        self.processed.add(url)
        if url in self.errors:
            del self.errors[url]
            self.save_errors()
        try:
            with open(self.files['processed'], 'w') as f: 
                json.dump(list(self.processed), f, indent=2)
        except Exception as e: logger.error(f"⚠️ Save Success Failed: {e}")

    def save_errors(self):
        try:
            with open(self.files['errors'], 'w') as f: 
                json.dump(self.errors, f, indent=2)
        except Exception as e: logger.error(f"⚠️ Save Errors Failed: {e}")

    def save_error(self, url, error_msg):
        self.errors[url] = str(error_msg)
        self.save_errors()
        
    async def post_init(self, application: Application):
        # 1. Identify Self
        me = await application.bot.get_me()
        self.bot_username = me.username
        logger.info(f"🤖 Identity Verified: @{self.bot_username}")

        # 2. Setup Namespaced File Paths
        self.files = {
            'processed': self.get_file_path("processed"),
            'errors': self.get_file_path("errors"),
            'queue': self.get_file_path("queue"),
            'topics': self.get_file_path("topics")
        }

        # 3. Load Persistent Data
        self.load_data()

        # 4. Auto-Configure Topics
        if TARGET_GROUP_ID and ERROR_GROUP_ID:
            topics_changed = False
            try:
                # Target Topic
                if not self.target_topic_id:
                    logger.info("🆕 Creating Target Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=TARGET_GROUP_ID, 
                        name=f"📚 {self.bot_username} Novels"
                    )
                    self.target_topic_id = topic.message_thread_id
                    topics_changed = True
                
                # Error Topic
                if not self.error_topic_id:
                    logger.info("🆕 Creating Error/Log Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=ERROR_GROUP_ID, 
                        name=f"🛠 {self.bot_username} Logs"
                    )
                    self.error_topic_id = topic.message_thread_id
                    topics_changed = True

                # Backup Topic
                if not self.backup_topic_id:
                    logger.info("🆕 Creating Backup Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=ERROR_GROUP_ID, 
                        name=f"🗄️ {self.bot_username} Backup"
                    )
                    self.backup_topic_id = topic.message_thread_id
                    topics_changed = True
                
                # Force save if we created new topics OR if we just migrated
                if topics_changed or not os.path.exists(self.files['topics']):
                    self.save_topics() 
                
                logger.info(f"🎯 Configuration: Target={self.target_topic_id} | Logs={self.error_topic_id}")

            except Exception as e:
                logger.error(f"❌ Failed to configure topics: {e}")

        # 5. Connect Userbot
        if SESSION_STRING and API_ID:
            try:
                self.userbot = UserBotClient(
                    "uploader",
                    api_id=int(API_ID),
                    api_hash=API_HASH,
                    session_string=SESSION_STRING,
                    in_memory=True
                )
                await self.userbot.start()
                logger.info("✅ Userbot Connected!")
            except Exception as e:
                logger.error(f"❌ Userbot Failed: {e}")

        # 6. Start Background Tasks
        asyncio.create_task(self.backup_loop(application.bot))

        # 7. Resume Pending Queue (With MIGRATION Logic)
        queue_path = self.files['queue']
        legacy_queue_path = os.path.join(DATA_DIR, "queue.json")

        # Check if legacy queue exists and new queue doesn't
        if not os.path.exists(queue_path) and os.path.exists(legacy_queue_path):
            logger.info("♻️ Migrating legacy queue.json to new format...")
            try:
                shutil.move(legacy_queue_path, queue_path)
            except Exception as e:
                logger.error(f"❌ Queue Migration Failed: {e}")

        # Process the queue if it exists
        if os.path.exists(queue_path):
            try:
                with open(queue_path, 'r') as f: data = json.load(f)
                urls = data.get("urls", [])
                pending = [u for u in urls if u not in self.processed]
                
                if pending:
                    msg = f"🔄 **Restarted**\nResuming {len(pending)} novels..."
                    await self.send_log(application.bot, msg)
                    asyncio.create_task(self.process_queue(urls, application.bot))
                else:
                    logger.info("✅ Queue file found but all novels processed. Cleaning up.")
                    os.remove(queue_path)
            except Exception as e:
                logger.error(f"❌ Error processing queue file: {e}")
        else:
            logger.info("ℹ️ No pending queue found.")

    # --- HELPER: Send Log ---
    async def send_log(self, bot, text, edit_msg=None):
        if ERROR_GROUP_ID and self.error_topic_id:
            try:
                if edit_msg:
                    return await edit_msg.edit_text(text)
                return await bot.send_message(
                    chat_id=ERROR_GROUP_ID, 
                    message_thread_id=self.error_topic_id, 
                    text=text
                )
            except: pass 
        return None

    # --- BACKUP SYSTEM ---
    async def backup_loop(self, bot):
        await asyncio.sleep(60) # Initial Delay
        while True:
            await self.perform_backup(bot)
            await asyncio.sleep(86400) # 24 Hours

    async def perform_backup(self, bot):
        if not ERROR_GROUP_ID or not self.backup_topic_id: return

        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            zip_name = f"backup_{self.bot_username}_{timestamp}.zip"
            zip_path = os.path.join(DATA_DIR, zip_name)
            
            # Backup only THIS bot's files
            files_to_backup = [
                f for f in os.listdir(DATA_DIR) 
                if f.endswith('.json') 
                and self.bot_username in f
                and os.path.isfile(os.path.join(DATA_DIR, f))
            ]
            
            if not files_to_backup: return

            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file_name in files_to_backup:
                    full_path = os.path.join(DATA_DIR, file_name)
                    zf.write(full_path, file_name)
            
            caption = (
                f"🗄️ **Daily Backup**\n"
                f"📅 {timestamp}\n"
                f"📂 Files: {len(files_to_backup)}"
            )

            with open(zip_path, 'rb') as f:
                await bot.send_document(
                    chat_id=ERROR_GROUP_ID,
                    message_thread_id=self.backup_topic_id,
                    document=f,
                    caption=caption
                )
            
            if os.path.exists(zip_path): os.remove(zip_path)
            logger.info("✅ Backup uploaded successfully")
            
        except Exception as e:
            logger.error(f"Backup Failed: {e}")
            await self.send_log(bot, f"⚠️ Backup Failed: {e}")

    # --- BOT LOGIC ---
    def start(self):
        if not TOKEN: return
        app = Application.builder().token(TOKEN).post_init(self.post_init).build()
        
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("reset", self.cmd_reset))
        app.add_handler(CommandHandler("backup", self.cmd_force_backup))
        app.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, self.handle_bot_dm))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! (Threads: {THREADS_PER_NOVEL})")
        app.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"⚡ **FanMTL Bot** ⚡\nProcessed: {len(self.processed)}\nUser: {self.bot_username}")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.processed = set()
        if os.path.exists(self.files['processed']): os.remove(self.files['processed'])
        if os.path.exists(self.files['queue']): os.remove(self.files['queue'])
        await update.message.reply_text("🗑️ History Reset.")

    async def cmd_force_backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("⏳ Starting manual backup...")
        await self.perform_backup(context.bot)
        await update.message.reply_text("✅ Backup sent to logs group.")

    async def handle_bot_dm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.message.caption
        if uid and uid in pending_uploads:
            pending_uploads[uid].set_result(update.message.document.file_id)
            del pending_uploads[uid]

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        file = await update.message.document.get_file()
        temp_path = os.path.join(DATA_DIR, "temp.json")
        await file.download_to_drive(temp_path)
        try:
            with open(temp_path, 'r', encoding='utf-8') as f: urls = json.load(f)
            
            with open(self.files['queue'], 'w') as f:
                json.dump({"chat_id": update.effective_chat.id, "urls": urls}, f, indent=2)
            
            await update.message.reply_text(f"✅ Received {len(urls)} novels. Check Log Group for progress.")
            await self.process_queue(urls, context.bot)
        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ Invalid JSON")
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    async def process_queue(self, urls, bot):
        # Explicit garbage collection before starting a large batch
        gc.collect()
        
        to_process = [u for u in urls if u not in self.processed]
        if not to_process:
            if os.path.exists(self.files['queue']): os.remove(self.files['queue'])
            await self.send_log(bot, "✅ **Queue Complete**")
            return

        await self.send_log(bot, f"📥 **Starting Batch**\nQueue: {len(to_process)}")
        
        for url in to_process:
            if url in self.processed: continue
            await self.process_novel(url, bot)
            # Explicit garbage collection after each novel to free memory immediately
            gc.collect()
        
        await self.send_log(bot, "✅ **All Tasks Finished**")
        if os.path.exists(self.files['queue']): os.remove(self.files['queue'])

    async def process_novel(self, url: str, bot):
        status_msg = await self.send_log(bot, f"⏳ **Starting:** {url}")
        
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
                        await self.send_log(bot, text, edit_msg=status_msg)
                        last_text = text; last_update = time.time()
                    except: pass
            except queue.Empty: await asyncio.sleep(0.5)

        try:
            epub_path = await future
            duration = int(time.time() - start_time)
            
            if epub_path and os.path.exists(epub_path):
                file_size_mb = os.path.getsize(epub_path) / (1024 * 1024)
                caption = f"📕 {os.path.basename(epub_path)}\n📦 {file_size_mb:.1f}MB | ⏱️ {duration}s"
                
                try: await status_msg.delete()
                except: pass

                dest_chat_id = TARGET_GROUP_ID if TARGET_GROUP_ID else ERROR_GROUP_ID
                dest_topic_id = self.target_topic_id

                if not dest_chat_id or not dest_topic_id:
                     await self.send_log(bot, f"❌ Configuration Error: Target Group/Topic missing for {url}")
                     return

                # Case 1: Use Userbot if file is large AND Userbot is active
                if file_size_mb > USERBOT_THRESHOLD and self.userbot:
                    prog_msg = await self.send_log(bot, f"⚠️ File is {file_size_mb:.1f}MB (> {USERBOT_THRESHOLD}MB)\n🚀 Uploading via Userbot...")
                    
                    try:
                        await self.userbot.send_document(
                            chat_id=int(dest_chat_id),
                            document=epub_path,
                            caption=caption,
                            message_thread_id=dest_topic_id
                        )
                        await prog_msg.delete()
                        self.save_success(url)
                    except Exception as e:
                        await self.send_log(bot, f"❌ Userbot Upload Failed: {e}", edit_msg=prog_msg)
                
                # Case 2: Use Standard Bot API
                else:
                    if file_size_mb >= 50:
                         await self.send_log(bot, f"❌ File {file_size_mb:.1f}MB exceeds 50MB limit and Userbot is not active/configured.")
                         self.save_error(url, "File > 50MB & No Userbot")
                    else:
                        with open(epub_path, 'rb') as f:
                            await bot.send_document(
                                chat_id=dest_chat_id,
                                message_thread_id=dest_topic_id,
                                document=f,
                                caption=caption
                            )
                        self.save_success(url)

                os.remove(epub_path)
            else:
                await self.send_log(bot, f"❌ Generation failed for {url}", edit_msg=status_msg)
        except Exception as e:
            logger.error(f"Fail: {url} -> {e}")
            try: await self.send_log(bot, f"❌ Error: {e}", edit_msg=status_msg)
            except: pass
            self.save_error(url, str(e))

    def _scrape_logic(self, url: str, progress_queue):
        app = App()
        try:
            progress_queue.put(f"🔍 Fetching info...")
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            
            if app.crawler: app.crawler.init_executor(THREADS_PER_NOVEL)

            if app.crawler.novel_cover:
                try:
                    headers = {"Referer": "https://www.fanmtl.com/", "User-Agent": "Mozilla/5.0"}
                    # REDUCED: Timeout reduced from 15s to 10s to free resources faster
                    response = app.crawler.scraper.get(app.crawler.novel_cover, headers=headers, timeout=10)
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
                if i % 40 == 0: progress_queue.put(f"🚀 {int(app.progress)}% ({i}/{total})")
            
            failed = [c for c in app.chapters if not c.body or len(c.body.strip()) < 20]
            if failed:
                progress_queue.put(f"⚠️ Fixing {len(failed)} chapters...")
                app.crawler.download_chapters(failed)
                failed = [c for c in app.chapters if not c.body or len(c.body.strip()) < 20]
                for c in failed: c.body = f"<h1>Chapter {c.id}</h1><p><i>[Content Missing]</i></p>"

            progress_queue.put("📦 Binding...")
            for fmt, f in app.bind_books(): return f
            return None

        except Exception as e: raise e
        finally: 
            # CRITICAL: Clean up heavy objects immediately
            app.destroy()
            del app
            gc.collect()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()