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
        # 1. THREADS: 50 threads for fetching Table of Contents
        self.init_executor(50)
        self.cleaner.bad_css.update({'div[align="center"]'})

        # 2. NETWORK: Aggressive retry strategy to handle speed bursts
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # --- TITLE ---
        possible_title = soup.select_one("h1.novel-title")
        self.novel_title = possible_title.text.strip() if possible_title else "Unknown Novel"

        # --- COVER ---
        # Matches <figure class="cover"><img ...>
        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            # Handle lazy-loaded images if src is a placeholder
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        # --- AUTHOR (Strict Filtering) ---
        # Matches <div class="author">...<span itemprop="author">
        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        if author_tag:
            text = author_tag.text.strip()
            # If the author text contains the site URL or http, force it to Unknown
            if "fanmtl" in text.lower() or "http" in text:
                self.novel_author = "Unknown"
            else:
                self.novel_author = text
        else:
            self.novel_author = "Unknown"

        # --- SUMMARY ---
        # Matches <div class="summary">...<div class="content">
        summary_div = soup.select_one(".summary .content")
        if summary_div:
            self.novel_synopsis = summary_div.get_text("\n\n").strip()
        else:
            self.novel_synopsis = "Summary not available."

        # --- CHAPTERS ---
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []
        
        # Pagination Logic
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if not pagination:
            self.parse_chapter_list(soup)
        else:
            # Extract params to fetch all pages
            last_page = pagination[-1]
            last_page_url = self.absolute_url(last_page["href"])
            common_page_url = last_page_url.split("?")[0]
            params = parse_qs(urlparse(last_page_url).query)
            
            try:
                page_count = int(params.get("page", [0])[0]) + 1
                wjm_param = params.get("wjm", [""])[0]
                
                futures = []
                for page in range(page_count):
                    page_url = f"{common_page_url}?page={page}&wjm={wjm_param}"
                    futures.append(self.executor.submit(self.get_soup, page_url))
                
                for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                    self.parse_chapter_list(page_soup)
            except Exception:
                logger.exception("Pagination failed, falling back to single page.")
                self.parse_chapter_list(soup)

        self.chapters.sort(key=lambda x: x["id"])

    def parse_chapter_list(self, soup):
        for a in soup.select("ul.chapter-list li a"):
            try:
                chap_id = len(self.chapters) + 1
                self.chapters.append(Chapter(
                    id=chap_id,
                    volume=1,
                    url=self.absolute_url(a["href"]),
                    title=a.select_one(".chapter-title").text.strip(),
                ))
            except: pass

    def download_chapter_body(self, chapter):
        soup = self.get_soup(chapter["url"])
        # Matches <div class="chapter-content">
        body = soup.select_one("#chapter-article .chapter-content")
        if not body: return ""
        
        # FIX: 'extract_contents' is the correct method name for TextCleaner
        return self.cleaner.extract_contents(body)
