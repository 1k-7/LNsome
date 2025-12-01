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
        self.init_executor(2) # Keep consistent with bot.py optimizations
        self.cleaner.bad_css.update({'div[align="center"]'})

        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=retry)
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
        try:
            possible_title = soup.select_one("h1.novel-title")
            if possible_title:
                self.novel_title = possible_title.text.strip()
            else:
                meta_title = soup.select_one('meta[property="og:title"]')
                self.novel_title = meta_title.get("content").strip() if meta_title else "Unknown Title"
        except:
            self.novel_title = "Unknown Title"

        # 3. "No Chapters" Detection (The "Nullcon" logic)
        is_zero_chapter = False
        try:
            read_btn = soup.select_one("#readchapterbtn")
            header_stats = soup.select_one(".novel-info .header-stats")
            
            if read_btn and "no chapter" in read_btn.get_text(" ", strip=True).lower():
                is_zero_chapter = True
            elif header_stats and "no chapters" in header_stats.get_text(" ", strip=True).lower():
                is_zero_chapter = True
        except: pass

        if is_zero_chapter:
            self.volumes = [{"id": 1, "title": "Volume 1"}]
            self.chapters = []
            logger.info("Verified 0-chapter novel. Returning.")
            return

        # 4. Metadata Extraction
        try:
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

            summary_div = soup.select_one(".summary .content")
            self.novel_synopsis = summary_div.get_text("\n\n").strip() if summary_div else ""
        except: pass

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # 5. Pagination (Crash-Proof)
        try:
            pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
            
            if pagination_links:
                last_page_link = pagination_links[-1] 
                href = last_page_link.get("href")
                
                if href:
                    common_url = self.absolute_url(href).split("?")[0]
                    query = parse_qs(urlparse(href).query)
                    
                    # PARANOID LIST ACCESS
                    page_param = query.get("page")
                    if page_param and len(page_param) > 0:
                        page_count = int(page_param[0]) + 1
                    else:
                        page_count = 1 
                        
                    wjm_param = query.get("wjm")
                    wjm = wjm_param[0] if (wjm_param and len(wjm_param) > 0) else ""

                    futures = []
                    for page in range(page_count):
                        url = f"{common_url}?page={page}&wjm={wjm}"
                        futures.append(self.executor.submit(self.get_soup, url))
                    
                    for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                        self.parse_chapter_list(page_soup)
                else:
                    self.parse_chapter_list(soup)
            else:
                self.parse_chapter_list(soup)

        except Exception as e:
            logger.error(f"Pagination error: {e}. Fallback to single page.")
            self.parse_chapter_list(soup)

        # 6. Safe Sort
        try:
            self.chapters.sort(key=lambda x: x["id"] if isinstance(x, dict) else getattr(x, "id", 0))
        except: pass

        if not self.chapters:
            logger.warning("No chapters found. Returning gracefully.")
            return

    def parse_chapter_list(self, soup):
        if not soup: return
        try:
            for a in soup.select("ul.chapter-list li a"):
                try:
                    self.chapters.append(Chapter(
                        id=len(self.chapters) + 1,
                        volume=1,
                        url=self.absolute_url(a["href"]),
                        title=a.select_one(".chapter-title").text.strip(),
                    ))
                except: pass
        except: pass

    def download_chapter_body(self, chapter):
        soup = self.get_soup(chapter["url"])
        body = soup.select_one("#chapter-article .chapter-content")
        if not body: return None
        return self.cleaner.extract_contents(body)
