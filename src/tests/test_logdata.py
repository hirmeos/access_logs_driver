from unittest import TestCase

from logdata import Request


class TestLogData(TestCase):

    def setUp(self):
        self.filename = "files/test_output.csv"
        # filter_groups = []
        # for m in modes:
        #     filters = (
        #         output_stream(self.filename),
        #         make_filters(m['regex']),
        #         m['regex']
        #     )
        # filter_groups.append(filters)
        # self.log_s = LogStream("logs", filter_groups)

    def test_request(self):
        result = Request(
            "34.90.253.37",
            "01/Jun/2023:12:10:56 +0000",
            "GET",
            "https://static/js/libs/raphael.js",
            200,
            32154,
            "https://testing_url",
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            True,
        )
        self.assertEqual(
            str(result),
            "Request 2023-06-01 12:10:56, 34.90.253.37, /js/libs/raphael.js"
        )

# def test_line_to_request(self):
    # result= """RETURNED 34.90.253.37  03/Jun/2023:06:22:50 +0000 -  GET -  /articles/cjs/thumbs/serve/7557/ -  200 -  7943 -  https://cjs.sljol.info/about/data-guidelines/ -  Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 -  True"""
    # reader = csv.reader(self.filename, delimiter=',')
    # request = []
    # for row in reader:
        # request.append(line_to_request(row))
    # pass
