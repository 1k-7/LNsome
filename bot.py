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
THREADS_PER_NOVEL = 50

# Group Configs
TARGET_GROUP_ID = os.getenv("TARGET_GROUP_ID") # For Novels
ERROR_GROUP_ID = os.getenv("ERROR_GROUP_ID")   # For Logs/Progress/Errors

# Userbot Config
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
# Threshold to switch to Userbot (in MB)
USERBOT_THRESHOLD = 40.0 

DATA_DIR = os.getenv("DATA_DIR", "data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ERRORS_FILE = os.path.join(DATA_DIR, "errors.json")
QUEUE_FILE = os.path.join(DATA_DIR, "queue.json")
TOPICS_FILE = os.path.join(DATA_DIR, "topics.json")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

pending_uploads = {}

class NovelBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        self.load_history()
        self.userbot = None
        self.bot_username = None
        
        # Topic IDs (Thread IDs)
        self.target_topic_id = None
        self.error_topic_id = None
        self.backup_topic_id = None

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
        
        # Load Topic IDs
        if os.path.exists(TOPICS_FILE):
            try:
                with open(TOPICS_FILE, 'r') as f:
                    data = json.load(f)
                    self.target_topic_id = data.get("target_topic_id")
                    self.error_topic_id = data.get("error_topic_id")
                    self.backup_topic_id = data.get("backup_topic_id")
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
        
    def save_topics(self):
        with open(TOPICS_FILE, 'w') as f:
            json.dump({
                "target_topic_id": self.target_topic_id,
                "error_topic_id": self.error_topic_id,
                "backup_topic_id": self.backup_topic_id
            }, f, indent=2)

    async def post_init(self, application: Application):
        me = await application.bot.get_me()
        self.bot_username = me.username
        logger.info(f"🤖 Bot Username: @{self.bot_username}")

        # --- SETUP TOPICS ---
        if TARGET_GROUP_ID and ERROR_GROUP_ID:
            try:
                # 1. Target Topic (Novels)
                if not self.target_topic_id:
                    logger.info("Creating Target Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=TARGET_GROUP_ID, 
                        name=f"📚 {self.bot_username} Novels"
                    )
                    self.target_topic_id = topic.message_thread_id
                
                # 2. Error/Log Topic
                if not self.error_topic_id:
                    logger.info("Creating Error/Log Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=ERROR_GROUP_ID, 
                        name=f"🛠 {self.bot_username} Logs"
                    )
                    self.error_topic_id = topic.message_thread_id

                # 3. Backup Topic
                if not self.backup_topic_id:
                    logger.info("Creating Backup Topic...")
                    topic = await application.bot.create_forum_topic(
                        chat_id=ERROR_GROUP_ID, 
                        name=f"🗄️ {self.bot_username} Backup"
                    )
                    self.backup_topic_id = topic.message_thread_id
                
                self.save_topics()
                logger.info(f"✅ Topics Configured. Target: {self.target_topic_id}, Backup: {self.backup_topic_id}")
            except Exception as e:
                logger.error(f"❌ Failed to create/load topics: {e}")

        # --- USERBOT SETUP ---
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
                logger.info("✅ Userbot Connected! Large file support ready.")
            except Exception as e:
                logger.error(f"❌ Userbot Failed: {e}")

        # --- START BACKGROUND TASKS ---
        # 1. Backup Scheduler (Starts after 1 hour, then every 24h)
        asyncio.create_task(self.backup_loop(application.bot))

        # 2. Resume Queue
        if os.path.exists(QUEUE_FILE):
            try:
                with open(QUEUE_FILE, 'r') as f: data = json.load(f)
                chat_id = data.get("chat_id") # Original requester
                urls = data.get("urls", [])
                pending = [u for u in urls if u not in self.processed]
                if pending:
                    msg = f"🔄 **Restarted**\nResuming {len(pending)} novels..."
                    await self.send_log(application.bot, msg)
                    asyncio.create_task(self.process_queue(urls, application.bot))
                else:
                    os.remove(QUEUE_FILE)
            except: pass

    # --- BACKUP SYSTEM ---
    async def backup_loop(self, bot):
        # Initial wait to let bot start up properly (e.g., 60 seconds)
        await asyncio.sleep(60)
        
        while True:
            await self.perform_backup(bot)
            # Wait 24 hours (86400 seconds)
            await asyncio.sleep(86400)

    async def perform_backup(self, bot):
        if not ERROR_GROUP_ID or not self.backup_topic_id: return

        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            zip_name = f"backup_{self.bot_username}_{timestamp}.zip"
            zip_path = os.path.join(DATA_DIR, zip_name)
            
            # Identify all JSON files in data dir (queue, errors, processed, topics, etc.)
            files_to_backup = [
                f for f in os.listdir(DATA_DIR) 
                if f.endswith('.json') and os.path.isfile(os.path.join(DATA_DIR, f))
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
            
            # Clean up local zip
            if os.path.exists(zip_path): os.remove(zip_path)
            logger.info("✅ Backup uploaded successfully")
            
        except Exception as e:
            logger.error(f"Backup Failed: {e}")
            await self.send_log(bot, f"⚠️ Backup Failed: {e}")

    # Helper to send to Error/Log Group
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

    def start(self):
        if not TOKEN: return
        app = Application.builder().token(TOKEN).post_init(self.post_init).build()
        
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("reset", self.cmd_reset))
        app.add_handler(CommandHandler("backup", self.cmd_force_backup)) # Added command for manual backup
        app.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, self.handle_bot_dm))
        
        print("🚀 Loading sources...")
        load_sources()
        print(f"✅ Bot online! (Threads: {THREADS_PER_NOVEL})")
        app.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"⚡ **FanMTL Bot** ⚡\nProcessed: {len(self.processed)}\n\nLogs -> Group {ERROR_GROUP_ID}\nNovels -> Group {TARGET_GROUP_ID}")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.processed = set()
        if os.path.exists(PROCESSED_FILE): os.remove(PROCESSED_FILE)
        if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
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
            
            with open(QUEUE_FILE, 'w') as f:
                json.dump({"chat_id": update.effective_chat.id, "urls": urls}, f, indent=2)
            
            await update.message.reply_text(f"✅ Received {len(urls)} novels. Check Log Group for progress.")
            await self.process_queue(urls, context.bot)
        except Exception as e:
            logger.error(f"File Error: {e}")
            await update.message.reply_text("❌ Invalid JSON")
        finally:
            if os.path.exists(temp_path): os.remove(temp_path)

    async def process_queue(self, urls, bot):
        to_process = [u for u in urls if u not in self.processed]
        if not to_process:
            if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
            await self.send_log(bot, "✅ **Queue Complete**")
            return

        await self.send_log(bot, f"📥 **Starting Batch**\nQueue: {len(to_process)}")
        
        for url in to_process:
            if url in self.processed: continue
            await self.process_novel(url, bot)
            gc.collect()
        
        await self.send_log(bot, "✅ **All Tasks Finished**")
        if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)

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

                # --- UPLOAD LOGIC ---
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
            app.destroy()
            gc.collect()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()