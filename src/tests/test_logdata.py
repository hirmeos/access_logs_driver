import csv
import gzip
import os
import pathlib as pl
from unittest import TestCase

from logdata import LogStream, Request
from process_download_logs import make_filters


class TestLogData(TestCase):
    def setUp(self) -> None:
        self.modes = [
            {
                "measure": "https://test.test-eu.org/abc/downloads/v1",
                "name": "download",
                "regex": [
                    (
                        "https://abcdef.hijkl.info/articles/10.\\d{4,9}"
                        "/[-._;()/:a-zA-Z0-9]+/galley/\\d+/download"
                    )
                ],
            },
            {
                "measure": "tag:test.eu,2018:test:abc-html",
                "name": "htmlreader",
                "regex": [
                    (
                        "https://abcdef.hijkl.info/articles/10.\\d{4,9}/"
                        "[-._;()/:a-zA-Z0-9]+"
                    ),
                    (
                        "https://abcdef.hijkl.info/articles/abstract/10.\\"
                        "d{4,9}/[-._;()/:a-zA-Z0-9]+"
                    ),
                ],
            },
        ]
        self.file_log_name = "test_access.log-20230606.gz"
        self.cache_dir = "tests/files/"
        self.logs_files = self.cache_dir + "logs_test/"
        self.url_prefix = "https://abcdef.hijkl.info"
        self.file_output_name = self.cache_dir + "test_output.csv"

    def tearDown(self):
        """Check whether the file has been created by any test and
        delete them, useful when running tests separately."""
        if self.file_exists(self.logs_files + self.file_log_name):
            os.remove(self.logs_files + self.file_log_name)
        if self.file_exists(self.logs_files + "test_access.log-NODATE.gz"):
            os.remove(self.logs_files + "test_access.log-NODATE.gz")
        if self.file_exists(self.file_output_name):
            os.remove(self.file_output_name)

    def create_file_log(self, file_name, content):
        file = gzip.open(self.logs_files + file_name, "wb")
        file.write(content)
        file.close()

    def file_exists(self, path):
        if not pl.Path(path).resolve().is_file():
            return False
        return True

    def test_request_class_returns_right_reuqest(self) -> None:
        """Test the class request. All the required values extracted from the
        log files will be converted to the needed request."""
        result = Request(
            "34.90.253.37",
            "01/Jun/2023:12:10:56 +0000",
            "GET",
            "https://static/js/lib/test.js",
            200,
            32154,
            "https://testing_url",
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        )
        self.assertEqual(
            str(result),
            (
                "Request 2023-06-01 12:10:56, 34.90.253.37, "
                "https://static/js/lib/test.js"
            )
        )

    def test_request_class_errors_response_code_and_content_length(
        self
    ) -> None:
        """Test the class request fails with the assertions."""
        with self.assertRaises(AssertionError):
            Request(
                "34.90.253.37",
                "01/Jun/2023:12:10:56 +0000",
                "GET",
                "https://static/js/libs/testing.js",
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
                "https://static/js/libs/testing.js",
                301,
                -1,  # Will fail because of content length
                "https://testing_url",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            )

    def test_request_class_errors_because_of_not_normalised_url(self) -> None:
        """Test the class request fails with the normalise url method."""
        with self.assertRaises(IndexError) as err:
            Request(
                "34.90.253.37",
                "01/Jun/2023:12:10:56 +0000",
                "GET",
                "",  # Will fail because of empty url
                200,
                32154,
                "f12345fake_url_test",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            )
        self.assertEqual(
            str(err.exception), ("Error parsing: , string index out of range")
        )

    def test_log_stream_creates_csv_file_successfully_one_match(self) -> None:
        """Test the class LogStream the same way as is executed
        from run() in src/process_download_logs.py including the
        output.csv file creation and it's contents."""

        content = str.encode(
            "abcdef.presstest.com:123 12.12.345.67 - - "
            '[03/Jun/2023:04:51:45 +0000] "GET /articles/'
            'abstract/12.3456/abc.v12a1.1234/ HTTP/1.1" '
            '200 49221 "-" "Mozilla/5.0 (Windows NT 10.0; '
            'Win64; x64; ab:123.4) Gecko/12345678 Firefox/112.0"'
        )
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            file.close()
            filter_groups.append(filters)
        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
        self.assertTrue(self.file_exists(self.file_output_name))
        # Assert the csv file has got the right content
        with open(self.file_output_name, "rt") as csvfile:
            reader = csv.reader(csvfile, delimiter=".")
            for row in reader:
                self.assertEqual(
                    row,
                    [
                        "2023-06-03 04:51:45,12",
                        "12",
                        "345",
                        "67,https://abcdef",
                        "hijkl",
                        "info/articles/abstract/10",
                        "4038/abcdef",
                        "v49i1",
                        "7705,Mozilla/5",
                        "0 (Windows NT 10",
                        "0; Win64; x64; rv:109",
                        "0) Gecko/12345678 Firefox/112",
                        "0",
                    ],
                )
        csvfile.close()

    def test_log_stream_no_match_file_structure_is_ok(self) -> None:
        """Test the class LogStream the same way as is executed
        from run() in src/process_download_logs.py including the
        output.csv file creation and it's contents."""
        content = str.encode(
            "abcdef.tyestset.com:123 12.12.345.67 - - "
            '[03/Jun/2023:04:51:45 +0000] "GET /test'
            '/test/12.3456/abc.v12a1.1234/ HTTP/1.1" '
            '200 49221 "-" "test/5.0 (Windows NT 10.0; '
            'Win64; x64; ab:123.4) test/20100101 test/112.0"'
        )
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            file.close()
            filter_groups.append(filters)
        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
        self.assertTrue(self.file_exists(self.file_output_name))
        # Assert the csv file has got the right content
        with open(self.file_output_name, "rt") as csvfile:
            reader = csv.reader(csvfile, delimiter=".")
            for row in reader:
                self.assertEqual(row, [""])
        csvfile.close()

    def test_log_stream_raises_error_no_matches_timestamp(self) -> None:
        """Test the class LogStream fails because there is no match.
        The file test_access.log-NODATE.gz should have a date
        at the end."""

        content = str.encode(
            "abcdef.presstest.com:123 12.12.345.67 - - "
            '[03/Jun/2023:04:51:45 +0000] "GET /articles/'
            'abstract/12.3456/abc.v12a1.1234/ HTTP/1.1" 200 '
            '49221 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; '
            'x64; ab:123.4) Gecko/12345678 Firefox/112.0"'
        )
        self.create_file_log(self.file_log_name, content)
        os.rename(
            self.logs_files + self.file_log_name,
            self.logs_files + "test_access.log-NODATE.gz",
        )
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            file.close()
            filter_groups.append(filters)

        with self.assertRaises(AttributeError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            "Your file has to have a date at the end of it's name"
        )

    def test_log_stream_raises_error_no_matches_lines(self) -> None:
        """Test the class LogStream fails because there is no match.
        The line method will raise exception because the structure of
        the input file is wrong."""
        content = b'test_worng - wrong structure"'
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            filter_groups.append(filters)
            file.close()

        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'not enough values to unpack (expected 5, got 4)'))"
            )
        )

    def test_log_stream_raises_error_no_matches_line_to_request(self) -> None:
        """Test the class LogStream fails because there is no match.
        The file structure is right but the request is wrong."""
        content = str.encode(
            "abcdef.presstest.com:123 12.12.345.67 - - "
            "[02/Jun/2023:08:18:56 +0000] --->ErrorHEAD "
            "/articles/abstract/12.3456/abcdef.v52i2.8160/ "
            'HTTP/1.1" 200 4522 "-" "Scoop.it"'
        )
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            filter_groups.append(filters)
            file.close()

        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            ),
        )
        content = str.encode(
            "abcdef.presstest.com:123 12.12.345.67 - - "
            "[02/Jun/2023:08:18:56 +0000] --->ErrorHEADtest "
            '"-" "Scoop.it"'
        )
        self.create_file_log(self.file_log_name, content)
        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            ),
        )

    def test_log_stream_raises_error_no_matches_line_r_n_ua_re(self) -> None:
        """Test the class LogStream fails because there is no match.
        The content of the files doesn't match r_n_ua_re regex."""
        content = str.encode(
            "abcdef.presstest.com:123 12.12.345.67 - - "
            '[01/Jun/2023:06:52:43 +0000] "GET /jms/public'
            '/journals/1/journalFavicon_en_US.ico HTTP/1.1" 404 '
            '5010 "test error!"test error!"'
        )
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            filter_groups.append(filters)
            file.close()

        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            "There wasn't any match with the url"
        )

    def test_log_stream_ignores_reuqest_if_its_missing_parts(self) -> None:
        """Test the class LogStream ignores urls that have a missing method,
        url or version."""
        content = str.encode(
            "abc.presstest.com:123 12.12.345.67 - - "
            '[01/Jun/2023:06:52:43 +0000] "GET /jms/public'
            '/journals/1/journalFavicon_en_US.ico" 404 5010 '
            '"https://abc.defg.info/'
            'abc.v52i2.8023/" "Mozilla/5.0 (Linux; Android 10; K) '
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
            '113.0.0.0 Mobile Safari/537.36"abc.presstest.'
            "com:123 12.12.345.67 - - [01/Jun/2023:06:52:50 +0000] "
            '"GET /jms/public/journals/1/journalFavicon_en_US.ico" '
        )
        self.create_file_log(self.file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            file = open(self.file_output_name, "w")
            filters = (
                file,
                make_filters(mode["regex"], ["8.8.8.8", "9.9.9.9"]),
                mode["regex"],
            )
            filter_groups.append(filters)
            file.close()

        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
