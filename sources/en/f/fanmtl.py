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
        # 50 threads is the safe sweet spot for a single novel
        self.init_executor(50)
        self.cleaner.bad_css.update({'div[align="center"]'})

        # Robust connection strategy
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # --- METADATA ---
        title_node = soup.select_one("h1.novel-title")
        self.novel_title = title_node.text.strip() if title_node else "Unknown Novel"

        # Cover: Prioritize figure, fallback to generic
        img = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img:
            url = img.get("src")
            if "placeholder" in str(url) and img.get("data-src"):
                url = img.get("data-src")
            self.novel_cover = self.absolute_url(url)

        # Author: Filter out URL junk
        author_node = soup.select_one('.novel-info .author span[itemprop="author"]')
        if author_node:
            text = author_node.text.strip()
            self.novel_author = "Unknown" if "http" in text or "fanmtl" in text.lower() else text
        else:
            self.novel_author = "Unknown"

        # Summary
        summary_node = soup.select_one(".summary .content")
        self.novel_synopsis = summary_node.get_text("\n\n").strip() if summary_node else ""

        # --- CHAPTERS ---
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []
        
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination:
            self.parse_chapter_list(soup)
        else:
            last_page = pagination[-1]
            common_url = self.absolute_url(last_page["href"]).split("?")[0]
            params = parse_qs(urlparse(last_page["href"]).query)
            
            try:
                # Fetch all TOC pages concurrently
                page_count = int(params.get("page", [0])[0]) + 1
                wjm = params.get("wjm", [""])[0]
                
                futures = []
                for page in range(page_count):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    futures.append(self.executor.submit(self.get_soup, url))
                
                for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                    self.parse_chapter_list(page_soup)
            except Exception:
                self.parse_chapter_list(soup)

        self.chapters.sort(key=lambda x: x["id"])

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
        # Correct usage: extract_contents
        return self.cleaner.extract_contents(body)
