import csv
import gzip
import os
import pathlib as pl
from unittest import TestCase

from logdata import LogStream, Request
from process_download_logs import (
    make_filters,
    output_stream
)


class TestLogData(TestCase):

    def setUp(self) -> None:
        self.file_name = "files/test_output.csv"
        self.modes = [
            {
                "measure": "https://metrics.operas-eu.org/obp/downloads/v1",
                "name": "download",
                "regex": [
                    (
                        "https://cjs.sljol.info/articles/10.\\d{4,9}/[-._;()/:a-zA-Z0-9]+/"
                        "galley/\\d+/download"
                    )
                ]
            },
            {
                "measure": "tag:operas.eu,2018:readership:obp-html",
                "name": "htmlreader",
                "regex": [
                    (
                        "https://cjs.sljol.info/articles/10.\\d{4,9}/"
                        "[-._;()/:a-zA-Z0-9]+"
                    ),
                    (
                        "https://cjs.sljol.info/articles/abstract/10.\\"
                        "d{4,9}/[-._;()/:a-zA-Z0-9]+"
                    )
                ]
            }
        ]
        self.cache_dir = "tests/files/"
        self.logs_files = self.cache_dir + "logs_test/"
        self.url_prefix = "https://cjs.sljol.info"
        self.file_output_name = self.cache_dir+"/test_output.csv"

    def create_file_log(self, file_name, content):
        file = gzip.open(self.logs_files+file_name, 'wb')
        file.write(content)
        file.close()

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError(f"File does not exist: {str(path)}")

    def test_request_class_returns_right_reuqest(self) -> None:
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
            (
                "Request 2023-06-01 12:10:56, 34.90.253.37, "
                "https://static/js/libs/raphael.js"
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
            str(err.exception),
            (
                "Error parsing: , string index out of range"
            )
        )

    def test_log_stream_creates_csv_file_successfully_one_match(
        self
    ) -> None:
        """Test the class LogStream the same way as is executed
        from run() in src/process_download_logs.py including the
        output.csv file creation and it's contents."""
        
        file_log_name = 'test_access.log-20230606.gz'
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [03/Jun/2023:04:51:45 +0000] "GET /articles/abstract/10.4038/cjs.v49i1.7705/ HTTP/1.1" 200 49221 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)
        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
        self.assertIsFile(self.file_output_name)
        # Assert the csv file has got the right content
        with open(self.file_output_name, 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter='.')
            for row in reader:
                self.assertEqual(
                    row,
                    [
                        '2023-06-03 04:51:45,34',
                        '90',
                        '253',
                        '37,https://cjs',
                        'sljol',
                        'info/articles/abstract/10',
                        '4038/cjs',
                        'v49i1',
                        '7705,Mozilla/5',
                        '0 (Windows NT 10',
                        '0; Win64; x64; rv:109',
                        '0) Gecko/20100101 Firefox/112',
                        '0'
                    ]
                )
        # Remove the log file and output
        os.remove(self.logs_files+file_log_name)
        os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_no_match_file_structure_is_ok(self) -> None:
        """Test the class LogStream the same way as is executed
        from run() in src/process_download_logs.py including the
        output.csv file creation and it's contents."""
        file_log_name = 'test_access.log-20230606.gz'
        content = b'cjs.tyestset.com:443 34.90.253.37 - - [03/Jun/2023:04:51:45 +0000] "GET /test/test/10.4038/cjs.v49i1.7705/ HTTP/1.1" 200 49221 "-" "test/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) test/20100101 test/112.0"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)
        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
        self.assertIsFile(self.file_output_name)
        # Assert the csv file has got the right content
        with open(self.file_output_name, 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter='.')
            for row in reader:
                self.assertEqual(
                    row,
                    ['']
                )
        # Remove the log file and output
        os.remove(self.logs_files+file_log_name)
        os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_raises_error_no_matches_timestamp(self) -> None:
        """Test the class LogStream fails because there is no match.
        The file gl_cjs_test_access.log-NODATE.gz should have a date
        at the end."""
        
        file_log_name = "test_access.log-20230606.gz"
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [03/Jun/2023:04:51:45 +0000] "GET /articles/abstract/10.4038/cjs.v49i1.7705/ HTTP/1.1" 200 49221 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0"'
        self.create_file_log(file_log_name, content)
        os.rename(
            self.logs_files + file_log_name,
            self.logs_files + "gl_cjs_test_access.log-NODATE.gz"
        )
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(AttributeError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            "Your file has to have a date at the end of it's name"
        )
        # Remove the log file and output
        os.remove(self.logs_files + "gl_cjs_test_access.log-NODATE.gz")
        os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_raises_error_no_matches_lines(self) -> None:
        """Test the class LogStream fails because there is no match.
        The line method will raise exception because the structure of
        the input file is wrong."""
        file_log_name = "test_access.log-20230606.gz"
        content = b'test_worng - wrong structure"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

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
        # Remove the log file and output
        os.remove(self.logs_files + file_log_name)
        os.remove(self.cache_dir+"/test_output.csv")

    def test_log_stream_raises_error_no_matches_line_to_request(self) -> None:
        """Test the class LogStream fails because there is no match.
        The file structure is right but the request is wrong."""
        file_log_name = "test_access.log-20230606.gz"
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [02/Jun/2023:08:18:56 +0000] --->ErrorHEAD /articles/abstract/10.4038/cjs.v52i2.8160/ HTTP/1.1" 200 4522 "-" "Scoop.it"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            )
        )
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [02/Jun/2023:08:18:56 +0000] --->ErrorHEADtestteststesestsetets "-" "Scoop.it"'
        self.create_file_log(file_log_name, content)
        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            (
                "('Your file has not the right structure, ', ValueError("
                "'Request column malformed'))"
            )
        )

    def test_log_stream_raises_error_no_matches_line_r_n_ua_re(
        self
    ) -> None:
        """Test the class LogStream fails because there is no match.
        The content of the files doesn't match r_n_ua_re regex."""
        file_log_name = "test_access.log-20230606.gz"
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [01/Jun/2023:06:52:43 +0000] "GET /jms/public/journals/1/journalFavicon_en_US.ico HTTP/1.1" 404 5010 "test error!!!"!!!"test error!!!"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        with self.assertRaises(ValueError) as err:
            result = LogStream(self.logs_files, filter_groups, self.url_prefix)
            files = result.to_csvs()
            [file.close() for file in files]
        self.assertEqual(
            str(err.exception),
            "There wasn't any match with the url"
        )

    def test_log_stream_ignores_reuqest_if_its_missing_parts(
        self
    ) -> None:
        """Test the class LogStream ignores urls that have a missing method,
         url or version."""
        file_log_name = "test_access.log-20230606.gz"
        content = b'cjs.ubiquitypress.com:443 34.90.253.37 - - [01/Jun/2023:06:52:43 +0000] "GET /jms/public/journals/1/journalFavicon_en_US.ico" 404 5010 "https://cjs.sljol.info/articles/abstract/10.4038/cjs.v52i2.8023/" "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Mobile Safari/537.36"cjs.ubiquitypress.com:443 34.90.253.37 - - [01/Jun/2023:06:52:50 +0000] "GET /jms/public/journals/1/journalFavicon_en_US.ico" 404 5010 "https://cjs.sljol.info/700/volume/52/issue/2/" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"cjs.ubiquitypress.com:443 34.90.253.37 - - [01/Jun/2023:06:52:54 +0000] "GET /static/js/libs/waypoints.js" 200 8796 "https://cjs.sljol.info/685/volume/49/issue/5/" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36"'
        self.create_file_log(file_log_name, content)
        filter_groups = []
        for mode in self.modes:
            filters = (
                output_stream(self.file_output_name),
                make_filters(mode['regex'], ["8.8.8.8", "9.9.9.9"]),
                mode['regex']
            )
            filter_groups.append(filters)

        result = LogStream(self.logs_files, filter_groups, self.url_prefix)
        files = result.to_csvs()
        [file.close() for file in files]
