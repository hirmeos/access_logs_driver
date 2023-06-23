import os
import pathlib as pl
from unittest import TestCase

from logdata import LogStream, Request
from process_download_logs import (
    make_filters,
    output_stream
)


class TestLogData(TestCase):

    def setUp(self):
        self.filename = "files/test_output.csv"
        self.modes = [
            {
                "measure": "https://metrics.operas-eu.org/obp/downloads/v1",
                "name": "download",
                "regex": [
                    (
                        "https:\\/\\/www\\.openbookpublishers\\.com\\/10.\\"
                        "d{4,9}/[-._;()/:A-Z0-9]+\\.pdf"
                    )
                ]
            },
            {
                "measure": "tag:operas.eu,2018:readership:obp-html",
                "name": "htmlreader",
                "regex": [
                    (
                        "^(?!.*(\\.png|\\.css|\\.json|\\.js|\\.jpg|\\.ttf|"
                        "\\.otf|\\.mp3|\\.mp4|\\.ttc)$)https:\\/\\/www\\"
                        ".openbookpublishers\\.com\\/htmlreader\\/"
                        "(?:[0-9]{3}-)?[0-9]{1,5}-[0-9]{1,7}-[0-9]{1,6}-[0-9]"
                    )
                ]
            }
        ]
        self.cache_dir = "tests/files"
        self.url_prefix = "https://www.openbookpublishers.com"

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError(f"File does not exist: {str(path)}")

    def test_request_class_returns_right_reuqest(self):
        """Test the class request. All the required values extracted from the
        log files will be converted to the needed request."""
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

    def test_request_class_errors(self):
        """Test the class request fails with the assertions."""
        with self.assertRaises(AssertionError):
            Request(
                "34.90.253.37",
                "01/Jun/2023:12:10:56 +0000",
                "GET",
                "https://static/js/libs/raphael.js",
                99,  # Will fail because of response code
                32154,
                "https://testing_url",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
                True,
            )
        with self.assertRaises(AssertionError):
            Request(
                "34.90.253.37",
                "01/Jun/2023:12:10:56 +0000",
                "GET",
                "https://static/js/libs/raphael.js",
                301,
                -1,  # Will fail because of content length
                "https://testing_url",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
                True,
            )

    def test_log_stream_creates_csv_file_successfully(self):
        """Test the class LogStream the same way as is executed
        from run() in src/process_download_logs.py including the
        output.csv file creation."""
        logs = self.cache_dir+"/logs/"
        filename = self.cache_dir+"/test_output.csv"
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)
        result = LogStream(logs, filter_groups, self.url_prefix)
        result.to_csvs()
        self.assertIsFile(self.cache_dir+"/test_output.csv")
        os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_raises_error_no_matches_timestamp(self):
        """Test the class LogStream fails because there is no match."""
        logs = self.cache_dir+"/logs/"
        filename = self.cache_dir+"/test_output.csv"
        # Rename the file
        if not os.path.isfile(logs + "gl_cjs_test_access.log-20230603.gz"):
            raise FileNotFoundError("Missing testing file")
        os.rename(
            logs + "gl_cjs_test_access.log-20230603.gz",
            logs + "gl_cjs_test_access.log-NODATE.gz"
        )
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(AttributeError):
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        os.rename(
            logs + "gl_cjs_test_access.log-NODATE.gz",
            logs + "gl_cjs_test_access.log-20230603.gz"
        )
