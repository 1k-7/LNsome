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
        self.init_executor(8)
        self.cleaner.bad_css.update({'div[align="center"]'})

        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # 1. Cloudflare Detection
        body_text = soup.body.text.lower() if soup.body else ""
        if "verify you are human" in body_text or "just a moment" in body_text:
            raise Exception("Cloudflare Blocked Request")

        # 2. Title Extraction
        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            # Fallback for broken pages
            self.novel_title = "Unknown Title"

        # 3. "No Chapters" Detection
        # If we find explicit "No chapter" text, we stop immediately.
        is_zero_chapter = False
        read_btn = soup.select_one("#readchapterbtn")
        header_stats = soup.select_one(".novel-info .header-stats")
        
        if read_btn and "no chapter" in read_btn.get_text(" ", strip=True).lower():
            is_zero_chapter = True
        elif header_stats and "no chapters" in header_stats.get_text(" ", strip=True).lower():
            is_zero_chapter = True
            
        if is_zero_chapter:
            self.volumes = [{"id": 1, "title": "Volume 1"}]
            self.chapters = []
            logger.info("Verified 0-chapter novel. Returning early.")
            return

        # 4. Metadata
        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        if author_tag:
            self.novel_author = author_tag.text.strip()
        else:
            self.novel_author = "Unknown"

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # 5. Pagination (CRASH-PROOF METHOD)
        try:
            # Only look for pagination in the specific list container
            pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
            
            if pagination_links:
                # Get the last link safely
                last_page_link = pagination_links[-1] 
                href = last_page_link.get("href")
                
                if href:
                    common_url = self.absolute_url(href).split("?")[0]
                    query = parse_qs(urlparse(href).query)
                    
                    # SAFELY get page count. 
                    # parse_qs returns a list, e.g., {'page': ['5']}.
                    # We verify the list exists AND has items before accessing [0].
                    page_param = query.get("page")
                    if page_param and len(page_param) > 0:
                        page_count = int(page_param[0]) + 1
                    else:
                        page_count = 1 # Default if param missing
                        
                    # SAFELY get wjm param
                    wjm_param = query.get("wjm")
                    wjm = wjm_param[0] if (wjm_param and len(wjm_param) > 0) else ""

                    futures = []
                    for page in range(page_count):
                        url = f"{common_url}?page={page}&wjm={wjm}"
                        futures.append(self.executor.submit(self.get_soup, url))
                    
                    for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                        self.parse_chapter_list(page_soup)
                else:
                    # Link existed but had no href? Parse current page.
                    self.parse_chapter_list(soup)
            else:
                # No pagination found? Parse current page.
                self.parse_chapter_list(soup)

        except Exception as e:
            # If ANYTHING goes wrong in pagination, log it and just parse the current page.
            # This prevents the crawler from crashing.
            logger.error(f"Pagination error: {e}. Fallback to single page.")
            self.parse_chapter_list(soup)

        # 6. Safe Sort
        # Handles objects or dicts safely
        self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))

        # 7. Final Safety Check
        if not self.chapters:
            logger.warning("Chapter list is empty. Returning empty list instead of crashing.")
            return

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
