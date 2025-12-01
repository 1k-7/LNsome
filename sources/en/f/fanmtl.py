# -*- coding: utf-8 -*-
import logging
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # FIX: Reduced threads from 40 to 8 to prevent RAM usage spike and Cloudflare bans
        self.init_executor(8)
        self.cleaner.bad_css.update({'div[align="center"]'})

        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # 1. Cloudflare/Block Detection
        body_text = soup.body.text.lower() if soup.body else ""
        block_keywords = [
            "just a moment", "attention required", "verify you are human", 
            "security check", "ray id", "enable javascript"
        ]
        if any(keyword in body_text for keyword in block_keywords):
            raise Exception("Cloudflare Blocked Request")

        # 2. Title Detection
        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            # If no title found, it's likely a broken page or a block.
            raise Exception("Failed to parse novel title - Possible Block or Layout Change")

        # 3. PRECISE ZERO-CHAPTER CHECK (Improved)
        is_zero_chapter = False
        
        # Method A: Specific Tag Structure
        # Structure: <span><strong><i class="..."></i> No</strong><small>Chapters</small></span>
        for small in soup.select("small"):
            if "Chapters" in small.get_text(strip=True):
                # find_previous_sibling("strong") skips whitespace/newlines to find the tag
                prev = small.find_previous_sibling("strong")
                if prev and "No" in prev.get_text(strip=True):
                    is_zero_chapter = True
                    break
        
        # Method B: Robust Text Fallback (New)
        # If the tag structure failed (e.g., extra whitespace, different nesting), check the text context.
        if not is_zero_chapter:
            # Find the header stats container (usually .novel-info or .header-stats)
            header_stats = soup.select_one(".novel-info") or soup.select_one(".header-stats")
            if header_stats:
                # Combine text with spaces to handle "No" and "Chapters" being in different tags
                stats_text = header_stats.get_text(" ", strip=True).lower()
                if "no chapters" in stats_text or "0 chapters" in stats_text:
                    is_zero_chapter = True
        
        if is_zero_chapter:
            self.volumes = [{"id": 1, "title": "Volume 1"}]
            self.chapters = []
            logger.info("Verified 0-chapter novel.")
            # CRITICAL: Return immediately. This results in empty chapters list 
            # without running pagination logic that might crash.
            return 

        # 4. Standard Metadata Extraction
        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        if author_tag:
            text = author_tag.text.strip()
            self.novel_author = "Unknown" if "http" in text or "fanmtl" in text.lower() else text
        else:
            self.novel_author = "Unknown"

        summary_div = soup.select_one(".summary .content")
        self.novel_synopsis = summary_div.get_text("\n\n").strip() if summary_div else ""

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []
        
        # 5. Pagination & Chapter Parsing (Safe Mode)
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        # We wrap this in try-except to catch the 'IndexError' if the site returns weird pagination data
        try:
            if pagination:
                last_page = pagination[-1]
                common_url = self.absolute_url(last_page["href"]).split("?")[0]
                params = parse_qs(urlparse(last_page["href"]).query)
                
                # SAFE EXTRACTION: Check if 'page' exists and has items before accessing [0]
                page_values = params.get("page", [])
                if page_values:
                    page_count = int(page_values[0]) + 1
                else:
                    page_count = 1 # Fallback if param is missing
                
                # Safe extraction for 'wjm'
                wjm_values = params.get("wjm", [""])
                wjm = wjm_values[0] if wjm_values else ""
                
                futures = []
                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    futures.append(self.executor.submit(self.get_soup, url))
                
                for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                    self.parse_chapter_list(page_soup)
            else:
                self.parse_chapter_list(soup)
        except Exception as e:
            logger.error(f"Pagination parsing failed: {e}. Falling back to single page list.")
            self.parse_chapter_list(soup)

        self.chapters.sort(key=lambda x: x["id"])

        # Final Verification
        if not self.chapters:
             # If we get here, the list is empty BUT we didn't find the "No Chapters" tag earlier.
             # This is a parsing failure. Raise Exception to trigger retry.
             raise Exception("Parsing Error: Chapter list empty but 'No Chapters' indicator missing")

    def parse_chapter_list(self, soup):
        for a in soup.select("ul.chapter-list li a"):
            try:
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=self.absolute_url(a["href"]),
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        soup = self.get_soup(chapter["url"])
        body = soup.select_one("#chapter-article .chapter-content")
        if not body: return None
        return self.cleaner.extract_contents(body)
