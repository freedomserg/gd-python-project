# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import os, sys
from datetime import datetime as dt

from scrapy import signals
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv
from scrapy.exporters import CsvItemExporter

from model import *
from gd_blog_parser.items import BlogPostItem, AuthorItem


class SqlitePipeline(object):
    """Represents a Scrapy pipeline for items produced by BlogSpider and AuthorSpider.
    Processes BlogPostItem(s), AuthorItem(s), stores new ones to SQL Lite db, performs updates on existing items
    if necessary.
    """

    def __init__(self, settings):
        self.logger = logging.getLogger("BLOG_SCRAPER_PIPELINE")
        self.logger.setLevel(logging.INFO)
        self.database = settings.get('DATABASE')
        self.sessions = {}
        self.blog_posts_first_run = True
        self.authors_first_run = True
        self.authors_to_process = {}
        self.latest_post_date = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def create_engine(self):
        """Creates an engine to communicate with db."""

        self.logger.debug("=" * 125)
        self.logger.debug("Creating DB engine")
        current_path = os.path.abspath(os.path.dirname(sys.argv[0]))
        engine = create_engine("sqlite:///{0}/{1}".format(current_path, self.database["database"]))
        return engine

    def create_tables(self, engine):
        """Creates all db tables if not exist."""

        self.logger.debug("=" * 125)
        self.logger.debug("Creating DB tables")
        DeclarativeBase.metadata.create_all(engine, checkfirst=True)

    def create_session(self, engine):
        """Creates a db session."""

        session = sessionmaker(bind=engine)()
        return session

    def _spider_opened_lifecycle_subsequent_run(self, lifecycle, session):
        """Populates some class fields on spider_opened signal when it's not the first run of a spider."""

        self.blog_posts_first_run = lifecycle.blog_posts_first_run
        self.authors_first_run = lifecycle.authors_first_run
        if self.authors_first_run:
            session.query(Lifecycle).update({Lifecycle.authors_first_run: False})
            session.commit()
        latest_blog_post = session.query(BlogPost).order_by(BlogPost.publication_date.desc()).first()
        self.latest_post_date = latest_blog_post.publication_date
        self.logger.info("Latest blog post publication date is {}".format(self.latest_post_date))

    def _spider_opened_lifecycle_first_run(self, spider, session):
        """Populates some class fields on spider_opened signal when it's the first run of a spider."""

        self.blog_posts_first_run = True
        self.authors_first_run = True
        if spider.name == "gd_blog_spider":
            new_lifecycle = Lifecycle(blog_posts_first_run=False, authors_first_run=True, new_blog_posts_found=False)
            try:
                session.add(new_lifecycle)
                session.commit()
                self.logger.info('New lifecycle {} stored in db'.format(new_lifecycle.blog_posts_first_run))
            except:
                self.logger.info('Failed to add {} to db'.format(new_lifecycle.blog_posts_first_run))
                session.rollback()
                raise

    def _update_lifecycle_if_blog_spider(self, spider, session):
        if spider.name == "gd_blog_spider":
            session.query(Lifecycle).update({Lifecycle.new_blog_posts_found: False})
            session.commit()

    def _handle_authors_to_process_if_author_spider(self, spider, session):
        """Retrieves names of authors that should be upserted by AuthorSpider."""

        if spider.name == "gd_author_spider":
            self.logger.debug("Retrieving authors to process")
            authors_to_process = session.query(AuthorToProcess).all()
            session.query(AuthorToProcess).delete()
            session.commit()
            self.logger.debug("Fetched {} items to process.".format(len(authors_to_process)))
            for item in authors_to_process:
                self.authors_to_process[item.full_name] = item.full_name

    def spider_opened(self, spider):
        """Performs necessary actions on spider_opened signal."""

        engine = self.create_engine()
        self.create_tables(engine)
        session = self.create_session(engine)
        self.sessions[spider] = session

        lifecycle = session.query(Lifecycle).first()
        if lifecycle:
            self._spider_opened_lifecycle_subsequent_run(lifecycle, session)
        else:
            self._spider_opened_lifecycle_first_run(spider, session)

        self._update_lifecycle_if_blog_spider(spider, session)
        self._handle_authors_to_process_if_author_spider(spider, session)

        # self.blogposts_file = open('blogposts.csv', 'w+b')
        # self.blogpost_exporter = CsvItemExporter(self.blogposts_file)
        # self.blogpost_exporter.start_exporting()
        #
        # self.authors_file = open('authors.csv', 'w+b')
        # self.author_exporter = CsvItemExporter(self.authors_file)
        # self.author_exporter.start_exporting()

    def check_new_blog_posts(self, spider, session):
        if spider.name == "gd_author_spider":
            lifecycle = session.query(Lifecycle).first()
            if not lifecycle.new_blog_posts_found:
                self.logger.warning(
                    "=================================================================  No new blog posts were found !")

    def store_authors_to_upsert_for_author_spider(self, spider, session):
        """Stores names of authors that should be upserted by AuthorSpider."""

        if spider.name == "gd_blog_spider":
            self.logger.info("Storing authors to process. Number is: {}".format(len(self.authors_to_process)))
            for key in self.authors_to_process:
                author_to_process = AuthorToProcess(full_name=key)
                try:
                    session.add(author_to_process)
                    session.commit()
                    self.logger.info('AuthorToProcess {} stored in db'.format(author_to_process))
                except:
                    self.logger.info('Failed to add {} to db'.format(author_to_process))
                    session.rollback()
                    raise

    def spider_closed(self, spider):
        """Performs necessary actions on spider_closed signal."""

        session = self.sessions.pop(spider)
        self.check_new_blog_posts(spider, session)
        self.store_authors_to_upsert_for_author_spider(spider, session)
        session.close()

        # self.blogpost_exporter.finish_exporting()
        # self.blogposts_file.close()
        #
        # self.author_exporter.finish_exporting()
        # self.authors_file.close()

    def store_new_blogpost(self, session, new_blog_post):
        """Stores:
         - a new blog post item in db
         - a flag that new blog post found
         """

        try:
            session.add(new_blog_post)
            session.commit()

            session.query(Lifecycle).update({Lifecycle.new_blog_posts_found: True})
            session.commit()
            self.logger.info('BlogPost {} stored in db'.format(new_blog_post.title))
        except:
            self.logger.info('Failed to add {} to db'.format(new_blog_post.title))
            session.rollback()
            raise

    def save_blogpost_to_db_and_return_item(self, session, new_blog_post, item):
        """Checks whether a blog post exists by a url. If not - stores a new blog post to db."""

        post_exists = session.query(BlogPost).filter_by(url=new_blog_post.url).first() is not None
        if post_exists:
            self.logger.info('BlogPost {} is already in db'.format(new_blog_post.title))
            return item

        self.store_new_blogpost(session, new_blog_post)
        return item

    def process_item(self, item, spider):
        """Main processing point.
        Receives BlogPostItem(s) and AuthorItem(s), stores new ones, making updates to existing ones if necessary.
        """

        session = self.sessions[spider]

        if isinstance(item, BlogPostItem):
            # self.blogpost_exporter.export_item(item)
            new_blog_post = BlogPost(**item)

            if self.blog_posts_first_run:
                self.save_blogpost_to_db_and_return_item(session, new_blog_post, item)
            else:
                if new_blog_post.publication_date > self.latest_post_date:
                    self.logger.info(
                        "================== Detected a new blog post with date later then in db ================")

                    blog_post_authors = new_blog_post.author.split("::")
                    for new_author in blog_post_authors:
                        self.authors_to_process[new_author] = new_author
                        self.logger.info("Author '{}' should be upserted.".format(new_author))
                    self.save_blogpost_to_db_and_return_item(session, new_blog_post, item)
                else:
                    return item

        if isinstance(item, AuthorItem):
            # self.author_exporter.export_item(item)
            author = Author(**item)

            if self.authors_first_run:
                self.process_author_and_return_item(session, author, item)
            else:
                if author.full_name in self.authors_to_process:
                    self.authors_to_process.pop(author.full_name)
                    self.process_author_and_return_item(session, author, item)
                else:
                    self.logger.debug("Author {} has no changes.".format(author.full_name))
                    return item
        return item

    def process_author_and_return_item(self, session, new_author, item):
        """Checks whether an author exists in db by its full name.
        If exists - updates articles_count field.
        Adds a new author item to db otherwise.
        """

        author_exists = session.query(Author).filter_by(full_name=item['full_name']).first() is not None
        try:
            if author_exists:
                self.logger.info('Author {} is already in db'.format(new_author.full_name))
                self.logger.info('Updating articles count up to {}'.format(new_author.articles_count))
                session.query(Author) \
                    .filter(Author.full_name == new_author.full_name) \
                    .update({Author.articles_count: new_author.articles_count})
                session.commit()
            else:
                session.add(new_author)
                session.commit()
                self.logger.info('Author {} stored in db'.format(new_author.full_name))
        except:
            self.logger.info('Failed to upsert {} in db'.format(new_author.full_name))
            session.rollback()
            raise
        return item
