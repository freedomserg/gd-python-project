import logging
import os, sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import *
from matplotlib import pyplot as plt
import pandas as pd

logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)

logger = logging.getLogger("BUILD_REPORT_SCRIPT")
logger.setLevel(logging.INFO)


def _create_db_session():
    current_path = os.path.abspath(os.path.dirname(sys.argv[0]))
    engine = create_engine("sqlite:///{0}/database.db".format(current_path))
    Session = sessionmaker(bind=engine)
    return Session()


def _get_posts_per_tag_df(initial_blog_posts_df):
    """Converts initial Dataframe of blog post items to a new Dataframe with 2 columns:
    tag: a name of a single tag
    posts: number of blog posts a tag is mentioned in
    """

    result_df = pd.DataFrame(columns=["tag", "posts"])
    for row in initial_blog_posts_df.itertuples(index=False):
        tags_list = row.tags
        post_title = row.title
        for tag in tags_list:
            if result_df["tag"].isin([tag]).any():
                result = result_df.loc[result_df["tag"].isin([tag])].head(1)
                result_tag = result.tag.values[0]
                result_posts = result.posts.values[0]
                result_posts.append(post_title)
                result_df.loc[result_df["tag"].isin([result_tag]), "posts"] = [result_posts]
            else:
                result_df = result_df.append({
                    "tag": tag,
                    "posts": [post_title]
                }, ignore_index=True)
    return result_df


def _get_top_7_tags_df(all_posts_df):
    """Converts initial Dataframe of blog post items to a new Dataframe with 2 columns:
    tag: a name of a single tag
    posts: number of blog posts a tag is mentioned in
    Resulting Dataframe consists of 7 top tags sorted by number of blog posts.
    """

    all_posts_df["tags"] = all_posts_df["tags"].apply(lambda tags_str: tags_str.split("::"))
    title_and_tags_df = all_posts_df[["title", "tags"]]
    posts_per_tag_df = _get_posts_per_tag_df(title_and_tags_df)
    posts_per_tag_df["posts"] = posts_per_tag_df["posts"].apply(lambda posts_list: len(posts_list))
    return posts_per_tag_df.sort_values("posts", ascending=False).head(7).iloc[::-1]


def _get_top_5_authors_df(session):
    """Reads Author items from SQL Lite db and returns a Dataframe consisting of 5 top authors sorted by number of
    posted articles.
    """

    all_authors_df = pd.read_sql(session.query(Author).statement, session.bind)
    return all_authors_df.sort_values("articles_count", ascending=False).head(5).iloc[::-1]


def _get_top_5_articles_df(all_posts_df):
    """Sorts initial Dataframe of blog post items by publication_date and returns a Dataframe with top 5 articles."""

    top_5_art_df = all_posts_df.sort_values("publication_date", ascending=False).head(5).iloc[::-1]
    top_5_art_df["publication_date"] = top_5_art_df["publication_date"].apply(
        lambda d: d.strftime("%m/%d/%Y"))
    return top_5_art_df


def _draw_report(top_tags_df, top_authors_df, top_articles_df):
    """Receives 3 Dataframes and draws a report."""

    fig, (ax1, ax2, ax3) = plt.subplots(3, figsize=(14, 7))
    width = 0.75
    fig.subplots_adjust(left=0.35, wspace=0.2, hspace=0.7)

    tags_ind = range(7)
    ax1.barh(tags_ind, top_tags_df.posts, width)
    ax1.set_yticks(tags_ind)
    ax1.set_yticklabels(top_tags_df.tag, minor=False)
    ax1.set_xlabel("Number of blog posts tagged")
    ax1.tick_params(axis='both', which='major', labelsize=8)
    ax1.set_title("TOP-7 tags")
    # for i, v in enumerate(top_7_tags_df.posts):
    #     ax1.text(v + 0.5, i - 0.3, str(v), color='black', fontsize=8, fontweight="bold")

    auth_ind = range(5)
    ax2.barh(auth_ind, top_authors_df.articles_count, width)
    ax2.set_yticks(auth_ind)
    ax2.set_yticklabels(top_authors_df.full_name, minor=False)
    ax2.tick_params(axis='both', which='major', labelsize=8)
    ax2.set_xlabel("Number of articles posted")
    ax2.margins(x=.2)
    ax2.set_title("TOP-5 authors")
    # for i, v in enumerate(top_5_authors_df.articles_count):
    #     ax2.text(v + 0.5, i - 0.1, str(v), color='black', fontsize=8, fontweight="bold")

    art_ind = range(1, 6)
    ax3.barh(art_ind, art_ind, width)
    ax3.set_yticks(art_ind)
    ax3.set_yticklabels(top_articles_df.title, minor=False)

    ax3.set_xticks(art_ind)
    ax3.set_xticklabels(top_articles_df.publication_date, minor=False, rotation=0)
    ax3.tick_params(axis='both', which='major', labelsize=8)
    ax3.set_xlabel("Publication date", fontsize=9)
    ax3.set_title("Latest posted articles")

    fig.suptitle("Report")
    plt.show()


def read_data_and_build_report():
    """Reads data from SQL Lite db, performs necessary transformations on it and draws a report."""

    logger.info("============================================ Processing data.")
    session = _create_db_session()

    all_blog_posts_df = pd.read_sql(session.query(BlogPost).statement, session.bind)
    top_7_tags_df = _get_top_7_tags_df(all_blog_posts_df)
    top_5_authors_df = _get_top_5_authors_df(session)
    top_5_articles_df = _get_top_5_articles_df(all_blog_posts_df)

    logger.info("============================================ Visualizing data.")
    _draw_report(top_7_tags_df, top_5_authors_df, top_5_articles_df)

    session.close()