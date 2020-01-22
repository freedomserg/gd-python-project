# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BlogPostItem(scrapy.Item):
    """Represents a scraped blog post entity"""

    title = scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()
    publication_date = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()


class AuthorItem(scrapy.Item):
    """Represents a scraped author entity"""

    full_name = scrapy.Field()
    job_title = scrapy.Field()
    linkedin_url = scrapy.Field()
    articles_count = scrapy.Field()
