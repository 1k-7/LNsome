# -*- coding: utf-8 -*-
import logging
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # 1. NUCLEAR SPEED: 100 threads for TOC
        self.init_executor(100)
        self.cleaner.bad_css.update({'div[align="center"]'})

        # 2. CRITICAL: Increase Connection Pool to match threads
        # Without this, you are capped at 10 speeds regardless of thread count
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=3)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # Title
        possible_title = soup.select_one("h1.novel-title")
        self.novel_title = possible_title.text.strip() if possible_title else "Unknown Novel"

        # Cover (Fixed Selector)
        img_tag = soup.select_one("figure.cover img")
        if not img_tag:
             img_tag = soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        # Author
        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        if author_tag:
            text = author_tag.text.strip()
            self.novel_author = text if "http" not in text else "Unknown"
        else:
            self.novel_author = "Unknown"

        # Summary
        summary_div = soup.select_one(".summary .content")
        if summary_div:
            self.novel_synopsis = summary_div.get_text("\n\n").strip()
        else:
            self.novel_synopsis = "Summary not available."

        # Volumes & Chapters
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []
        
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination:
            self.parse_chapter_list(soup)
        else:
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
            except Exception as e:
                logger.error("Pagination error: %s", e)
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
            except Exception:
                pass

    def download_chapter_body(self, chapter):
        soup = self.get_soup(chapter["url"])
        body = soup.select_one("#chapter-article .chapter-content")
        # Must use extract_contents
        return self.cleaner.extract_contents(body)
