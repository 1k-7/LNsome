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
            # Fallback: Try meta tag
            meta_title = soup.select_one('meta[property="og:title"]')
            if meta_title:
                self.novel_title = meta_title.get("content", "Unknown Title").strip()
            else:
                raise Exception("Failed to parse novel title - Possible Block or Layout Change")

        # 3. PRECISE ZERO-CHAPTER CHECK
        # We verify this explicitly to avoid unnecessary processing or errors.
        is_zero_chapter = False
        
        # Check A: "Read" button text (e.g., "Read No chapter")
        read_btn = soup.select_one("#readchapterbtn")
        if read_btn and "no chapter" in read_btn.get_text(strip=True).lower():
            is_zero_chapter = True
            logger.info("Detected 'No chapter' in read button.")

        # Check B: Header Stats (e.g., "No Chapters" or "0 Chapters")
        if not is_zero_chapter:
            header_stats = soup.select_one(".novel-info") or soup.select_one(".header-stats")
            if header_stats:
                stats_text = header_stats.get_text(" ", strip=True).lower()
                if "no chapters" in stats_text or "0 chapters" in stats_text:
                    is_zero_chapter = True
                    logger.info("Detected 'No Chapters' in header stats.")
        
        if is_zero_chapter:
            self.volumes = [{"id": 1, "title": "Volume 1"}]
            self.chapters = []
            logger.info("Verified 0-chapter novel. Returning immediately.")
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
        
        # 5. Pagination & Chapter Parsing (Crash-Proof Logic)
        try:
            # Locate pagination links strictly within the pagination container
            pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
            
            # Safe access helper to prevent IndexError
            def get_last(lst):
                return lst[-1] if lst and len(lst) > 0 else None
            
            def get_first(lst):
                return lst[0] if lst and len(lst) > 0 else None

            if pagination_links:
                last_page = get_last(pagination_links)
                href = last_page.get("href") if last_page else None
                
                if href:
                    common_url = self.absolute_url(href).split("?")[0]
                    query_params = parse_qs(urlparse(href).query)
                    
                    # CRITICAL FIX: Robustly extract page count. 
                    # Default to 1 if 'page' param is missing or empty.
                    page_param = get_first(query_params.get("page", []))
                    if page_param and str(page_param).isdigit():
                        page_count = int(page_param) + 1
                    else:
                        page_count = 1
                    
                    # Safe extraction of 'wjm' parameter
                    wjm = get_first(query_params.get("wjm", [])) or ""
                    
                    futures = []
                    for page in range(page_count):
                        url = f"{common_url}?page={page}&wjm={wjm}"
                        futures.append(self.executor.submit(self.get_soup, url))
                    
                    for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                        self.parse_chapter_list(page_soup)
                else:
                    # Link found but no href, fallback to current page
                    self.parse_chapter_list(soup)
            else:
                # No pagination links found, parse the current page
                self.parse_chapter_list(soup)

        except Exception as e:
            logger.error(f"Pagination error: {e}. Fallback to single page.")
            # If pagination fails for any reason, try to parse the current page
            # so we at least get something (or nothing, safely).
            self.parse_chapter_list(soup)

        # Safe Sort
        try:
            self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))
        except Exception:
            pass # Ignore sort errors if IDs are missing

        # 6. Final Verification
        if not self.chapters:
             # If chapters are empty, we do a final safety check.
             # If the raw HTML contains indicators of "no chapter", we assume it's valid and verify it now.
             # This catches cases where the layout was slightly different but still meant "0 chapters".
             if "no chapter" in str(soup).lower():
                 logger.info("Empty list & 'no chapter' found in HTML. Treating as 0-chapter novel.")
                 return

             # Only raise exception if we are SURE it's a failure and NOT just an empty novel
             raise Exception("Parsing Error: Chapter list empty but 'No Chapters' indicator missing")

    def parse_chapter_list(self, soup):
        if not soup: return
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
