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

        # 1. Cloudflare/Block Check
        body_text = soup.body.text.lower() if soup.body else ""
        block_keywords = [
            "just a moment", "attention required", "verify you are human", 
            "security check", "ray id", "enable javascript"
        ]
        if any(keyword in body_text for keyword in block_keywords):
            raise Exception("Cloudflare Blocked Request")

        # 2. Title Check
        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            raise Exception("Failed to parse novel title - Possible Block or Layout Change")

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
        
        # 3. Chapter Parsing (No Try/Except - Let it crash if layout breaks)
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if not pagination:
            self.parse_chapter_list(soup)
        else:
            last_page = pagination[-1]
            common_url = self.absolute_url(last_page["href"]).split("?")[0]
            params = parse_qs(urlparse(last_page["href"]).query)
            
            page_count = int(params.get("page", [0])[0]) + 1
            wjm = params.get("wjm", [""])[0]
            
            futures = []
            for page in range(page_count):
                url = f"{common_url}?page={page}&wjm={wjm}"
                futures.append(self.executor.submit(self.get_soup, url))
            
            for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                self.parse_chapter_list(page_soup)

        self.chapters.sort(key=lambda x: x["id"])

        # 4. STRICT 0-CHAPTER VALIDATION
        if not self.chapters:
            # FIX: Normalize text to handle split tags like <strong>No</strong> <small>Chapters</small>
            # soup.get_text(" ", strip=True) turns that into "No Chapters"
            full_text = soup.get_text(" ", strip=True).lower()
            
            if "no chapters" in full_text:
                pass # Verified 0-chapter novel. Returns empty list -> saved to nullcon.
            else:
                # If "No Chapters" isn't found, it's a layout error or block.
                raise Exception("Parsing Error: Chapter list empty but 'No Chapters' text missing")

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