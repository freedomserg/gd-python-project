import os, logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from gd_blog_parser.spiders.gd_blog_spider import BlogSpider
from gd_blog_parser.spiders.gd_author_spider import AuthorSpider

logger = logging.getLogger("RUN_SPIDERS_SCRIPT")
logger.setLevel(logging.INFO)


class Scraper:
    """Launches spiders sequentially in the same process."""

    def __init__(self):
        settings_file_path = 'gd_blog_parser.settings'
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        self.process = CrawlerProcess(get_project_settings())
        self.blog_spider = BlogSpider
        self.author_spider = AuthorSpider

    def start_sequentially(self, process: CrawlerProcess, spiders: list):
        logger.info("============================================  Start spider {}".format(spiders[0].__name__))
        deferred = process.crawl(spiders[0])
        if len(spiders) > 1:
            deferred.addCallback(lambda _: self.start_sequentially(process, spiders[1:]))

    def run_spiders(self):
        spiders = [self.blog_spider, self.author_spider]
        self.start_sequentially(self.process, spiders)
        self.process.start()