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
        )
        self.assertEqual(
            str(result),
            "Request 2023-06-01 12:10:56, 34.90.253.37, /js/libs/raphael.js"
        )

    def test_request_class_errors_response_code_and_content_length(self):
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
            )

    def test_request_class_errors_because_of_not_normalised_url(self):
        """Test the class request fails with the normalise url method."""
        with self.assertRaises(BaseException) as err:
            Request(
                "34.90.253.37",
                "01/Jun/2023:12:10:56 +0000",
                "GET",
                "",  # Will fail because of not a url
                200,
                32154,
                "f12345fake_url_test",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            )
        self.assertEqual(
            str(err.exception),
            (
                "Error parsing: , The url is malformed"
            )
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
        # os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_raises_error_no_matches_timestamp(self):
        """Test the class LogStream fails because there is no match.
        The file gl_cjs_test_access.log-NODATE.gz should have a date 
        at the end."""
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

        with self.assertRaises(AttributeError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        self.assertEqual(
            str(err.exception),
            "Your file has to have a date at the end of it's name"
        )

        os.rename(
            logs + "gl_cjs_test_access.log-NODATE.gz",
            logs + "gl_cjs_test_access.log-20230603.gz"
        )

    def test_log_stream_raises_error_no_matches_lines(self):
        """Test the class LogStream fails because there is no match.
        The file gl_cjs_test_access.log-NODATE.gz should have a date 
        at the end."""
        logs = self.cache_dir+"/logs/fail_test_files_1/"
        filename = self.cache_dir+"/test_output.csv"

        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'not enough values to unpack (expected 5, got 3)'))"
            )
        )

    def test_log_stream_raises_error_no_matches_line_to_request(self):
        """Test the class LogStream fails because there is no match.
        The content of the files don't have a correct request."""
        logs = self.cache_dir+"/logs/fail_test_files_2/"
        filename = self.cache_dir+"/test_output.csv"
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        print(err.exception)
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            )
        )
        logs = self.cache_dir+"/logs/fail_test_files_3/"
        with self.assertRaises(ValueError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            )
        )

    def test_log_stream_raises_error_no_matches_line_r_n_ua_re(
        self
    ):
        """Test the class LogStream fails because there is no match.
        The content of the files don't have a correct request."""
        logs = self.cache_dir+"/logs/fail_test_files_5/"
        filename = self.cache_dir+"/test_output.csv"
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        self.assertEqual(
            str(err.exception),
            "There wasn't any match with the url"
        )

    def test_log_stream_raises_error_no_unpack_url(
        self
    ):
        """Test the class LogStream fails because there is no match.
        The content of the files don't have a correct request."""
        logs = self.cache_dir+"/logs/fail_test_files_5/"
        filename = self.cache_dir+"/test_output.csv"
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(filename),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(logs, filter_groups, self.url_prefix)
            result.to_csvs()
        self.assertEqual(
            str(err.exception),
            "There wasn't any match with the url"
        )

    def test_log_stream_ignores_reuqest_if_its_missing_parts(
        self
    ):
        """Test the class LogStream ignores urls that are missing method,
         url or version."""
        logs = self.cache_dir+"/logs/fail_test_files_7/"
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
