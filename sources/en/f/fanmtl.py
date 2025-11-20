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
        self.init_executor(10)
        self.cleaner.bad_css.update({'div[align="center"]'})

    def parse_title(self, soup: BeautifulSoup) -> str:
        possible_title = soup.select_one(".novel-info .novel-title, h1.novel-title")
        assert possible_title, "No novel title"
        return possible_title.text.strip()

    def parse_cover(self, soup: BeautifulSoup) -> str:
        # Try multiple selectors for the cover image
        possible_image = (
            soup.select_one(".novel-header figure.cover img") or
            soup.select_one(".novel-left .novel-cover img") or
            soup.select_one("img[itemprop='image']")
        )
        if possible_image:
            return self.absolute_url(possible_image.get("src") or possible_image.get("data-src"))

    def parse_authors(self, soup: BeautifulSoup) -> Generator[str, None, None]:
        possible_author = soup.select_one('.novel-info .author span[itemprop="author"]')
        if possible_author:
            text = possible_author.text.strip()
            # Prevent the URL from being listed as the author
            if "fanmtl" in text.lower() or "http" in text:
                yield "Unknown"
            else:
                yield text
        else:
            yield "Unknown"

    def parse_synopsis(self, soup: BeautifulSoup) -> str:
        # Grab the summary/description
        possible_synopsis = soup.select_one(".novel-info .description, .description, .summary")
        if possible_synopsis:
            return possible_synopsis.text.strip()
        return "Summary not available."

    def select_chapter_tags(self, soup: BeautifulSoup) -> Generator[Tag, None, None]:
        # Check for pagination to avoid IndexErrors on single-page novels
        pagination = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        
        if not pagination:
            yield from soup.select("ul.chapter-list li a")
            return

        last_page = pagination[-1]
        last_page_url = self.absolute_url(last_page["href"])
        
        common_page_url = last_page_url.split("?")[0]
        params = parse_qs(urlparse(last_page_url).query)
        
        try:
            page_count = int(params.get("page", [0])[0]) + 1
            wjm_param = params.get("wjm", [""])[0]
        except (IndexError, ValueError):
            yield from soup.select("ul.chapter-list li a")
            return

        futures = []
        for page in range(page_count):
            page_url = f"{common_page_url}?page={page}&wjm={wjm_param}"
            futures.append(self.executor.submit(self.get_soup, page_url))
            
        for soup in self.resolve_futures(futures, desc="TOC", unit="page"):
            yield from soup.select("ul.chapter-list li a")

    def parse_chapter_item(self, tag: Tag, id: int) -> Chapter:
        return Chapter(
            id=id,
            url=self.absolute_url(tag["href"]),
            title=tag.select_one(".chapter-title").text.strip(),
        )

    def select_chapter_body(self, soup: BeautifulSoup) -> Tag:
        return soup.select_one("#chapter-article .chapter-content")
