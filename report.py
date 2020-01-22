import subprocess
import logging
import os, sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import *
from run_scraper import Scraper
from build_report import read_data_and_build_report
from dateutil.parser import parse as parse_date

logger = logging.getLogger("MAIN_SCRIPT")
logger.setLevel(logging.INFO)

# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)


def main():
    """
       The main entry point of the application
    """

    # current_path = os.path.abspath(os.path.dirname(sys.argv[0]))
    # engine = create_engine("sqlite:///{0}/database.db".format(current_path))
    #
    # Session = sessionmaker(bind=engine)
    # session = Session()

    # date_to_delete = "Jan 21, 2020"
    # date_to_delete2 = "Jan 16, 2020"
    # date_to_delete3 = "Jan 09, 2020"
    # to_delete = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete)).first()
    # to_delete2 = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete2)).first()
    # to_delete3 = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete3)).first()
    # logger.info("Blogpost to delete {}".format(to_delete))
    # logger.info("Blogpost to delete {}".format(to_delete2))
    # logger.info("Blogpost to delete {}".format(to_delete3))
    # deleted = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete)).delete()
    # deleted2 = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete2)).delete()
    # deleted3 = session.query(BlogPost).filter(BlogPost.publication_date == parse_date(date_to_delete3)).delete()
    # session.commit()
    # deleted_overall = int(deleted) + int(deleted2) + int(deleted3)
    # logger.info("Deleted {} items".format(deleted_overall))
    # session.close()

    # all_posts = session.query(BlogPost).order_by(BlogPost.publication_date.asc()).all()
    # for post in all_posts:
    #     logger.info("Existing post: {}".format(post))
    # logger.info("Blog posts number: {}".format(len(all_posts)))
    #
    # authors_to_process = session.query(AuthorToProcess).all()
    # for a in authors_to_process:
    #     logger.info("Author to process: {}".format(a))
    #
    # session.close()

    # all_authors = session.query(Author).all()
    # articles_number = 0
    # for author in all_authors:
    #     logger.warning("Existing author: {}".format(author))
    #     articles_number += author.articles_count
    # logger.warning("Number of articles from all authors: {}".format(articles_number))

    # session.close()


    # logger.debug("=" * 125)
    # logger.debug("Crawling started.")
    #
    # # scrapy_process = subprocess.Popen(["scrapy", "crawl", "gd_blog_spider"], stdout=subprocess.PIPE)
    # logger.debug("=" * 125)
    # logger.debug("Waiting for Scrapy process to be completed.")
    # return_code = scrapy_process.wait()
    #
    # if return_code == 0:
    #     logger.debug("=" * 125)
    #     logger.info("Scrapy process is completed.")
    #
    # else:
    #     logger.debug("=" * 125)
    #     logger.error("Scrapy process failed.")

    scraper = Scraper()
    try:
        logger.info("============================================ Start collecting data from Grid Dynamics blog post.")
        scraper.run_spiders()
        logger.info("============================================ Collecting data completed.")
    except Exception as e:
        logger.error("Exception while crawling: {}".format(e))

    read_data_and_build_report()


if __name__ == "__main__":
    main()
