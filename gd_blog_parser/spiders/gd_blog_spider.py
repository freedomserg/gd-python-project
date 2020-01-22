import logging
import re
from dateutil.parser import parse as parse_date
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from scrapy.spiders import Spider

from gd_blog_parser.items import BlogPostItem


class BlogSpider(Spider):
    """Scrapy spider that parses the main blog page and produces BlogPostItem(s)"""

    name = "gd_blog_spider"
    start_urls = ["https://blog.griddynamics.com"]

    def __init__(self, *args, **kwargs):
        logging.getLogger("scrapy.core.engine").setLevel(logging.INFO)
        logging.getLogger("scrapy.core.scraper").setLevel(logging.INFO)
        logging.getLogger("scrapy.downloadermiddlewares.robotstxt").setLevel(logging.INFO)
        super().__init__(*args, **kwargs)

    def parse(self, response):
        """Main function fetching each blog post or series url and performing additional requests to a specific pages.
        Callback function of such request intended to parse a page of a concrete article or a page of series.
        """

        single_articles = response.css("div.blog.list > div.inner > div#firstlist > article")
        series_articles = response.xpath('//article[contains (@class, "series")]')

        for raw_post in single_articles:
            onclick_attr = raw_post.css("::attr(onclick)").extract_first()
            post_link_match_group = re.search('location.href=\'(.+?)\';', onclick_attr)
            if post_link_match_group:
                post_link = post_link_match_group.group(1)
            else:
                bad_post_id = raw_post.css("::attr(id)").extract_first()
                raise ValueError("Blog post url not found ! Post ID: {}".format(bad_post_id))

            yield scrapy.Request(url="https://blog.griddynamics.com{}".format(post_link), callback=self.parse_article)

        for serie in series_articles:
            onclick_attr_tag = serie.css("::attr(onclick)").extract_first()
            serie_link_match_group = re.search('location.href=\'(.+?)\';', onclick_attr_tag)
            if serie_link_match_group:
                serie_link = serie_link_match_group.group(1)
            else:
                bad_serie_id = serie.css("::attr(id)").extract_first()
                raise ValueError("Series url not found ! Serie ID: {}".format(bad_serie_id))

            yield scrapy.Request(url="https://blog.griddynamics.com{}".format(serie_link), callback=self.parse_series)

    def parse_article(self, response):
        """Parses an article page fetching necessary data and produces BlogPostItem."""

        raw_post = response.css("div.blog.post > div.inner > div.row > article")

        post_loader = ItemLoader(item=BlogPostItem(), selector=raw_post)
        post_loader.default_output_processor = TakeFirst()

        post_title = raw_post.css("div#postcontent > h1::text").extract_first()
        post_loader.add_value("title", post_title)
        post_loader.add_value("url", response.request.url)

        post_text_selector = raw_post.css("div#postcontent > div#mypost")
        post_text = post_text_selector.xpath('string(.)').extract_first()
        post_loader.add_value("content", post_text[:160])

        pub_date_text = raw_post.css("div#postcontent > div.no-mobile > div.posttag.right.nomobile > span::text").extract_first()
        pub_date = parse_date(pub_date_text)
        post_loader.add_value("publication_date", pub_date)

        initial_author_list = raw_post.css(
            "div#postcontent > div.no-mobile > div.postauthor > span > a.goauthor > span::text").extract()
        author_list = [name.strip() for name in initial_author_list]
        post_authors = "::".join(author_list)
        post_loader.add_value("author", post_authors)

        post_tags = raw_post.css("div#postcontent > a.tag.secondary::attr(title)").extract()
        post_tags_str = "::".join(post_tags)
        post_loader.add_value("tags", post_tags_str)

        return post_loader.load_item()

    def parse_series(self, response):
        """Parses a page of series retrieving urls of concrete articles and making necessary requests to those pages.
        Callback function of such request intended to parse a page of a concrete article.
        """

        articles = response.css("div.blog.list.taglist > div.inner > div#secondlist > article")

        for raw_post in articles:
            onclick_attr = raw_post.css("::attr(onclick)").extract_first()
            serie_link_match_group = re.search('location.href=\'(.+?)\';', onclick_attr)
            if serie_link_match_group:
                serie_link = serie_link_match_group.group(1)
            else:
                bad_serie_id = raw_post.css("::attr(id)").extract_first()
                raise ValueError("Series url not found ! Series ID: {}".format(bad_serie_id))

            yield scrapy.Request(url="https://blog.griddynamics.com{}".format(serie_link), callback=self.parse_article)


