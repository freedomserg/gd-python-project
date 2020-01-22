import logging
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from gd_blog_parser.items import AuthorItem
from scrapy.spiders import Spider


class AuthorSpider(Spider):
    """Scrapy spider that parses a page with all blog authors and produces AuthorItem(s)"""

    name = "gd_author_spider"
    start_urls = ["https://blog.griddynamics.com/all-authors/"]

    def __init__(self, *args, **kwargs):
        logging.getLogger("scrapy.core.engine").setLevel(logging.INFO)
        logging.getLogger("scrapy.core.scraper").setLevel(logging.INFO)
        logging.getLogger("scrapy.downloadermiddlewares.robotstxt").setLevel(logging.INFO)
        super().__init__(*args, **kwargs)

    def parse(self, response):
        """Main function fetching each author url and performing additional requests to authors' pages.
        Callback function of such request intended to parse a concrete author's page.
        """

        authors = response.css("div.blog.list.authorslist > div.inner > div.row > div.left > div.single")

        for raw_author in authors:
            author_page_link = raw_author.css("a.authormore::attr(href)").extract_first()
            yield scrapy.Request(url="https://blog.griddynamics.com{}".format(author_page_link), callback=self.parse_author)

    def parse_author(self, response):
        """Parses an author's page fetching necessary data and produces AuthorItem."""

        raw_author = response.css("div#authorbox > div.nomobile > div.right")

        author_loader = ItemLoader(item=AuthorItem(), selector=raw_author)
        author_loader.default_output_processor = TakeFirst()

        full_name = raw_author.css("h1::text").extract_first().strip()
        author_loader.add_value("full_name", full_name)

        job_title_str = raw_author.css("h2::text").extract_first()
        if job_title_str:
            job_title = job_title_str
        else:
            job_title = "No"
        author_loader.add_value("job_title", job_title)

        linkedin_url_str = raw_author.css("div.authorsocial > a.linkedin::attr(href)").extract_first()
        if linkedin_url_str:
            linkedin_url = linkedin_url_str
        else:
            linkedin_url = "No"
        author_loader.add_value("linkedin_url", linkedin_url)

        articles_count = len(response.css("div#authorbox > did.postlist > a"))
        author_loader.add_value("articles_count", articles_count)

        return author_loader.load_item()


