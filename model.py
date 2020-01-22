from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean

DeclarativeBase = declarative_base()


class Lifecycle(DeclarativeBase):
    __tablename__ = "lifecycle"
    id = Column(Integer, primary_key=True)
    blog_posts_first_run = Column('blog_posts_first_run', Boolean)
    authors_first_run = Column('authors__first_run', Boolean)
    new_blog_posts_found = Column('new_blog_posts_found', Boolean)

    def __repr__(self):
        return "<Lifecycle(id - {0}, blog_posts_first_run - {1}, authors_first_run - {2}, new_blog_posts_found - {3})>"\
            .format(self.id, self.blog_posts_first_run, self.authors_first_run, self.new_blog_posts_found)


class AuthorToProcess(DeclarativeBase):
    __tablename__ = "authors_to_process"
    id = Column(Integer, primary_key=True)
    full_name = Column('full_name', String)

    def __repr__(self):
        return "<AuthorToProcess({})>".format(self.full_name)


class BlogPost(DeclarativeBase):
    __tablename__ = "blogposts"
    id = Column(Integer, primary_key=True)
    title = Column('title', String)
    url = Column('url', String)
    content = Column('content', String)
    publication_date = Column('publication_date', DateTime)
    author = Column('author', String)
    tags = Column('tags', String)

    def __repr__(self):
        return "<BlogPost({0} :: {1} :: {2})>".format(self.title, self.url, self.publication_date)


class Author(DeclarativeBase):
    __tablename__ = "authors"
    id = Column(Integer, primary_key=True)
    full_name = Column('full_name', String)
    job_title = Column('job_title', String)
    linkedin_url = Column('linkedin_url', String)
    articles_count = Column('articles_count', Integer)

    def __repr__(self):
        return "<Author({0} --- {1}. Linkedin: {2}. Number of articles: {3})>".format(self.full_name, self.job_title, self.linkedin_url, self.articles_count)
