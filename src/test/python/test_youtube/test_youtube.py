from unittest import TestCase
from main.services.youtube.youtube import *


class TestYoutube(TestCase):

    def setUp(self):
        argparser.add_argument("--q", help="Search term", default="Google")
        argparser.add_argument("--max-results", help="Max results", default=25)
        self.args, unknown = argparser.parse_known_args()

    def test_youtube_search(self):
        print 3
        Youtube().youtube_search(self.args)
