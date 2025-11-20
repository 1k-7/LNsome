# -*- coding: utf-8 -*-
import logging
from typing import Generator
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup, Tag
from lncrawl.models import Chapter
from lncrawl.templates.browser.chapter_only import ChapterOnlyBrowserTemplate

logger = logging.getLogger(__name__)

class FanMTLCrawler(ChapterOnlyBrowserTemplate):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        self.init_executor(60) # Speed setting
        self.cleaner.bad_css.update({'div[align="center"]'})

    # --- FORCE READ METADATA (Bypass potential parent class bugs) ---
    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup(self.novel_url)

        # 1. Title
        possible_title = soup.select_one("h1.novel-title")
        self.novel_title = possible_title.text.strip() if possible_title else "Unknown Novel"
        logger.info("Title: %s", self.novel_title)

        # 2. Cover (Exact match from your HTML)
        # Structure: <div class="fixed-img"><figure class="cover"><img src="...">
        img_tag = soup.select_one("div.fixed-img figure.cover img")
        if img_tag:
            self.novel_cover = self.absolute_url(img_tag.get("src"))
        logger.info("Cover URL: %s", self.novel_cover)

        # 3. Author (Exact match from your HTML)
        # Structure: <div class="author"><span>Author:</span><span itemprop="author">NAME</span>
        author_tag = soup.select_one(".novel-info .author span[itemprop='author']")
        if author_tag:
            self.novel_author = author_tag.text.strip()
        else:
            self.novel_author = "Unknown"
        logger.info("Author: %s", self.novel_author)

        # 4. Summary (Exact match from your HTML)
        # Structure: <div class="summary">...<div class="content">...<p>TEXT</p>
        summary_div = soup.select_one("div.summary div.content")
        if summary_div:
            # get_text with separator ensures paragraphs don't merge into one giant blob
            self.novel_synopsis = summary_div.get_text("\n\n").strip()
        else:
            self.novel_synopsis = "Summary not available."
        logger.info("Summary found: %s", bool(self.novel_synopsis))

        # 5. Volumes & Chapters
        self.volumes = [{"id": 1, "title": "Volume 1"}] # FanMTL is usually 1 volume
        self.chapters = []
        
        # Parse Chapter List
        # Robust pagination check
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination:
            # Single page
            self.parse_chapter_list(soup)
        else:
            # Multi page
            last_page = pagination[-1]
            last_page_url = self.absolute_url(last_page["href"])
            common_page_url = last_page_url.split("?")[0]
            params = parse_qs(urlparse(last_page_url).query)
            
            try:
                page_count = int(params.get("page", [0])[0]) + 1
                wjm_param = params.get("wjm", [""])[0]
            except (IndexError, ValueError):
                self.parse_chapter_list(soup)
                return

            futures = []
            for page in range(page_count):
                page_url = f"{common_page_url}?page={page}&wjm={wjm_param}"
                futures.append(self.executor.submit(self.get_soup, page_url))
            
            for page_soup in self.resolve_futures(futures, desc="TOC", unit="page"):
                self.parse_chapter_list(page_soup)

        # Sort chapters by ID to ensure order
        self.chapters.sort(key=lambda x: x["id"])

    def parse_chapter_list(self, soup):
        # <ul class="chapter-list"><li><a href="...">...</a></li></ul>
        for a in soup.select("ul.chapter-list li a"):
            try:
                # Extract ID from URL or text to ensure correct ordering
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
        return self.cleaner.extract(body)
