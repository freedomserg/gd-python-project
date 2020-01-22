import unittest
import pandas as pd
from dateutil.parser import parse as parse_date
from build_report import _get_top_7_tags_df, _get_top_5_articles_df


class TestReportData(unittest.TestCase):

    def setUp(self):
        self.initial_data = {
            "title": [
                "Blog_post_1",
                "Blog_post_2",
                "Blog_post_3",
                "Blog_post_4",
                "Blog_post_5",
                "Blog_post_6",
                "Blog_post_7",
                "Blog_post_8"],
            "url": [
                "https://post-1",
                "https://post-2",
                "https://post-3",
                "https://post-4",
                "https://post-5",
                "https://post-6",
                "https://post-7",
                "https://post-8"],
            "content": ["post-1", "post-2", "post-3", "post-4", "post-5", "post-6", "post-7", "post-8"],
            "publication_date": [
                parse_date("Dec 01, 2019"),
                parse_date("Dec 02, 2019"),
                parse_date("Dec 03, 2019"),
                parse_date("Dec 04, 2019"),
                parse_date("Dec 05, 2019"),
                parse_date("Dec 06, 2019"),
                parse_date("Dec 07, 2019"),
                parse_date("Dec 08, 2019")],
            "author": [
                "John Smith",
                "Mikky Mouse",
                "John Doe",
                "Frodo Baggins",
                "Oliver Stone",
                "John Smith",
                "Mikky Mouse",
                "John Smith"],
            "tags": [
                "tag1::tag2::tag7::tag8::tag4::tag5::tag9",
                "tag6::tag1::tag2:tag3::tag9::tag10::tag5",
                "tag1::tag2::tag4::tag5::tag3::tag10",
                "tag1::tag3::tag5::tag2",
                "tag3::tag5::tag1::tag4::tag2",
                "tag9::tag4::tag2::tag5::tag1",
                "tag3::tag4::tag2::tag5::tag1",
                "tag1"]
        }

    def test_tags_ranged(self):
        all_posts_df = pd.DataFrame(data=self.initial_data)
        result = _get_top_7_tags_df(all_posts_df).values

        item_1 = result[0]
        self.assertEqual(item_1[0], "tag10")
        self.assertEqual(item_1[1], 2)

        item_2 = result[1]
        self.assertEqual(item_2[0], "tag9")
        self.assertEqual(item_2[1], 3)

        item_3 = result[2]
        self.assertEqual(item_3[0], "tag3")
        self.assertEqual(item_3[1], 4)

        item_4 = result[3]
        self.assertEqual(item_4[0], "tag4")
        self.assertEqual(item_4[1], 5)

        item_5 = result[4]
        self.assertEqual(item_5[0], "tag2")
        self.assertEqual(item_5[1], 6)

        item_6 = result[5]
        self.assertEqual(item_6[0], "tag5")
        self.assertEqual(item_6[1], 7)

        item_7 = result[6]
        self.assertEqual(item_7[0], "tag1")
        self.assertEqual(item_7[1], 8)

    def test_articles_ranged(self):
        all_posts_df = pd.DataFrame(data=self.initial_data)
        result = _get_top_5_articles_df(all_posts_df).values

        item_1 = result[0]
        self.assertEqual(item_1[0], "Blog_post_4")
        self.assertEqual(item_1[3], "12/04/2019")

        item_2 = result[1]
        self.assertEqual(item_2[0], "Blog_post_5")
        self.assertEqual(item_2[3], "12/05/2019")

        item_3 = result[2]
        self.assertEqual(item_3[0], "Blog_post_6")
        self.assertEqual(item_3[3], "12/06/2019")

        item_4 = result[3]
        self.assertEqual(item_4[0], "Blog_post_7")
        self.assertEqual(item_4[3], "12/07/2019")

        item_5 = result[4]
        self.assertEqual(item_5[0], "Blog_post_8")
        self.assertEqual(item_5[3], "12/08/2019")
