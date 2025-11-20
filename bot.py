cat > bot.py <<EOL
import os
import json
import logging
import asyncio
import shutil
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Import lncrawl core components
from lncrawl.core.app import App
from lncrawl.core.sources import prepare_crawler

# Configuration
TOKEN = os.getenv("TELEGRAM_TOKEN")
MAX_WORKERS = 10  # Speed boost
DOWNLOAD_DIR = "downloads"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")

    def start(self):
        if not TOKEN:
            print("Error: TELEGRAM_TOKEN environment variable not set.")
            return

        application = Application.builder().token(TOKEN).build()
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("Bot is polling...")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Welcome! Please upload a JSON file with FanMTL URLs."
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

            await update.message.reply_text(f"Starting batch process for {len(urls)} novels... 🚀")

            for url in urls:
                await self.process_novel(url, update, context)

            await update.message.reply_text("✅ All tasks completed.")

        except Exception as e:
            logger.error(f"Error: {e}")
            await update.message.reply_text("An error occurred.")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    async def process_novel(self, url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_msg = await update.message.reply_text(f"⏳ Processing: {url}")
        loop = asyncio.get_running_loop()
        try:
            epub_path = await loop.run_in_executor(self.executor, self._scrape_logic, url)
            if epub_path and os.path.exists(epub_path):
                await status_msg.edit_text(f"⬆️ Uploading: {url}")
                await update.message.reply_document(document=open(epub_path, 'rb'))
                await status_msg.delete()
                os.remove(epub_path)
            else:
                await status_msg.edit_text(f"❌ Failed: {url}")
        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {str(e)}")

    def _scrape_logic(self, url: str):
        app = App()
        try:
            app.user_input = url
            app.prepare_search()
            app.get_novel_info()
            if app.crawler:
                app.crawler.init_executor(MAX_WORKERS)
            app.chapters = app.crawler.chapters[:]
            app.pack_by_volume = False
            app.output_formats = {'epub': True}
            for _ in app.start_download(): pass
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