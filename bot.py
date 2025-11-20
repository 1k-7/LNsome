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
# Get this from @BotFather
TOKEN = os.getenv("TELEGRAM_TOKEN")
MAX_WORKERS = 10  # Speed boost: Override FanMTL's default 1 thread
DOWNLOAD_DIR = "downloads"

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

class NovelBot:
    def __init__(self):
        # Executor for running lncrawl (which is blocking) in a separate thread
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="bot_worker")

    def start(self):
        if not TOKEN:
            print("Error: TELEGRAM_TOKEN environment variable not set.")
            return

        application = Application.builder().token(TOKEN).build()

        # Handlers
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), self.handle_json_file))
        
        print("Bot is polling...")
        application.run_polling()

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Welcome! 📚\n\n"
            "Please upload a **JSON file** containing a list of FanMTL novel URLs.\n"
            "Example format:\n"
            "`[\"https://www.fanmtl.com/novel/example1\", \"https://www.fanmtl.com/novel/example2\"]`",
            parse_mode="Markdown"
        )

    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        document = update.message.document
        file = await document.get_file()
        
        # Download JSON
        file_path = os.path.join(DOWNLOAD_DIR, f"{document.file_id}.json")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        await file.download_to_drive(file_path)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                urls = json.load(f)
            
            if not isinstance(urls, list):
                await update.message.reply_text("Error: JSON must contain a list of URLs `[\"url1\", \"url2\"]`.")
                return

            await update.message.reply_text(f"Received {len(urls)} novels. Starting batch process... 🚀")

            # Process each URL
            for url in urls:
                await self.process_novel(url, update, context)

            await update.message.reply_text("✅ All tasks completed.")

        except json.JSONDecodeError:
            await update.message.reply_text("Error: Invalid JSON file.")
        except Exception as e:
            logger.error(f"File handling error: {e}")
            await update.message.reply_text("An unexpected error occurred processing the file.")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    async def process_novel(self, url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_msg = await update.message.reply_text(f"⏳ Initializing: {url}")
        
        # Run the blocking scraping task in a separate thread
        loop = asyncio.get_running_loop()
        try:
            epub_path = await loop.run_in_executor(self.executor, self._scrape_logic, url)
            
            if epub_path and os.path.exists(epub_path):
                await status_msg.edit_text(f"⬆️ Uploading EPUB for: {url}")
                await update.message.reply_document(document=open(epub_path, 'rb'))
                await status_msg.delete()
                
                # Cleanup
                os.remove(epub_path)
            else:
                await status_msg.edit_text(f"❌ Failed to generate EPUB for: {url}")

        except Exception as e:
            logger.error(f"Processing error for {url}: {e}")
            await status_msg.edit_text(f"❌ Error processing {url}: {str(e)}")

    def _scrape_logic(self, url: str):
        app = App()
        try:
            logger.info(f"Starting scrape for: {url}")
            app.user_input = url
            app.prepare_search()
            
            # 1. Initialize Crawler
            app.get_novel_info()
            
            # 2. SPEED HACK: Override the executor to use more threads
            # FanMTLCrawler defaults to 1 worker. We force it to MAX_WORKERS.
            if app.crawler:
                 # Re-initialize the internal executor of the crawler with more workers
                app.crawler.init_executor(MAX_WORKERS)

            # 3. Configure for "All Chapters" and "Single EPUB"
            app.chapters = app.crawler.chapters[:] # Select all
            app.pack_by_volume = False # Single file
            app.output_formats = {'epub': True} # Only EPUB

            # 4. Download
            # We iterate the generator to exhaust it (perform download)
            for _ in app.start_download():
                pass
            
            # 5. Bind (Generate EPUB)
            generated_files = []
            for fmt, file_path in app.bind_books():
                generated_files.append(file_path)
            
            if generated_files:
                # Move file to a temp location to avoid deletion by app.destroy()
                final_path = os.path.join(DOWNLOAD_DIR, os.path.basename(generated_files[0]))
                shutil.copy(generated_files[0], final_path)
                return final_path
            
            return None

        except Exception as e:
            logger.error(f"Scrape logic failed: {e}")
            raise e
        finally:
            # Cleanup the lncrawl app session
            app.destroy()

if __name__ == "__main__":
    bot = NovelBot()
    bot.start()