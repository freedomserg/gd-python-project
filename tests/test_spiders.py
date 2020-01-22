import unittest
from gd_blog_parser.spiders.gd_blog_spider import BlogSpider
from tests.fake_response import fake_response_from_file
from dateutil.parser import parse as parse_date
from scrapy.http import Request


class TestBlogSpider(unittest.TestCase):

    def setUp(self):
        self.spider = BlogSpider()

    def test_parse(self):
        blog_url = "https://blog.griddynamics.com/"
        result_gen = self.spider.parse(fake_response_from_file("html/gd_blog.html", blog_url))
        for request in result_gen:
            self.assertTrue(isinstance(request, Request))
            self.assertTrue(
                request.callback == self.spider.parse_article or request.callback == self.spider.parse_series)

    def test_parse_article(self):
        article_url = "https://blog.griddynamics.com/semantic-vector-search-the-new-frontier-in-product-discovery/"
        result_item = self.spider.parse_article(fake_response_from_file(
            "html/semantic_vector_search.html", article_url))
        self.assertIsNotNone(result_item["title"])
        expected_title = "Semantic vector search: the new frontier in product discovery"
        self.assertEqual(result_item["title"], expected_title)

        self.assertIsNotNone(result_item["url"])
        expected_url = article_url
        self.assertEqual(result_item["url"], expected_url)

        self.assertIsNotNone(result_item["content"])
        expected_content = "Deep-learning powered natural language processing is growing by leaps and bounds. " \
                           "During the past year, latest NLP models exceeded the human performance baselin"
        self.assertEqual(result_item["content"], expected_content)

        self.assertIsNotNone(result_item["publication_date"])
        expected_pub_date = parse_date("Jan 09, 2020")
        self.assertEqual(result_item["publication_date"], expected_pub_date)

        self.assertIsNotNone(result_item["author"])
        expected_author = "Eugene Steinberg"
        self.assertEqual(result_item["author"], expected_author)

        self.assertIsNotNone(result_item["tags"])
        expected_tags = "Machine Learning and Artificial Intelligence::E-commerce::Search"
        self.assertEqual(result_item["tags"], expected_tags)

    def test_parse_series(self):
        series_url = "https://blog.griddynamics.com/tag/introduction-to-augmented-reality/"
        result_gen = self.spider.parse_series(fake_response_from_file(
            "html/augment_reality_series.html", series_url))
        expected_urls = ["https://blog.griddynamics.com/how-arkit-and-arcore-recognize-vertical-planes/",
                         "https://blog.griddynamics.com/latest-arcore-and-sceneform-features-take-creation-of-ar-apps-to-the-next-level/",
                         "https://blog.griddynamics.com/introducing-augmented-reality-for-e-commerce/"]
        request_counter = 0
        for request in result_gen:
            request_counter += 1
            self.assertTrue(isinstance(request, Request))
            self.assertTrue(request.url in expected_urls)
            self.assertEqual(
                request.callback, self.spider.parse_article, msg="All requests should have 'parse_article' callback")
        self.assertTrue(request_counter == 3, msg="Expected 3 articles in series")


