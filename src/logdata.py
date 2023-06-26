#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Parse access logs filtering requests and normalising them.
"""

import re
import os
import csv
import sys
import time
import datetime
import subprocess
from typing import Iterator
import urllib.error
import urllib.parse

from requests import request

URL_PREFIX = os.environ.get('URL_PREFIX')


class Request(object):
    """Represent the data in a single line of the Apache log file."""
    def __init__(
        self,
        ip_address: str,
        timestamp: str,
        method: str,
        url: str,
        response_code: int,
        content_length: int,
        referer: str,
        user_agent: str,
    ):
        assert response_code >= 100
        assert response_code < 1000
        assert content_length >= 0

        self.ip_address = ip_address
        self.timestamp = time.strptime(timestamp[:20], '%d/%b/%Y:%H:%M:%S')
        self.method = method
        self.url = self.normalise_url(self.parse_url(url))
        self.response_code = response_code
        self.content_length = content_length
        self.referer = referer
        self.user_agent = user_agent

    @staticmethod
    def normalise_url(url):
        """We just avoid the malformed urls from the line_to_request
        'if len(request.split()) >= 3:' but we have to be safe to don't save
        wrong values."""
        try:
            u = URL_PREFIX + url.lower()
            return u[:-1] if u[-1] == "/" else u
        except BaseException:
            print("Error parsing: " + url, file=sys.stderr)
            raise

    @staticmethod
    def parse_url(url: str) -> str:
        try:
            if url.startswith("http"):
                return urllib.parse.urlparse(url).path
            return url
        except ValueError:
            raise ValueError(f"Error parsing: {url}, {sys.stderr}")

    def fmttime(self) -> datetime:
        fmt = "%Y-%m-%d %H:%M:%S"
        return datetime.datetime(*self.timestamp[:6]).strftime(fmt)

    def __str__(self) -> str:
        return f"Request {self.fmttime()}, {self.ip_address}, {self.url}"

    def as_tuple(self) -> tuple[datetime.datetime, int, str]:
        return (self.fmttime(), self.ip_address, self.url, self.user_agent)

    def sanitise_url(self, regexes: str) -> None:
        for regex in regexes:
            matched = re.search(re.compile(regex), self.url)
            if matched is not None:
                self.url = matched.group(0)
                break


class LogStream(object):
    def __init__(self, log_dir: str, filter_groups: list) -> None:
        self.log_dir = log_dir
        self.filter_groups = filter_groups

    request_re = re.compile(r'^(.*[^\\]") ([0-9]+) ([0-9]+) (.*)$')
    r_n_ua_re = re.compile(r'^"(.*)" "(.*)" *$')
    fallback_re = r'^()" ([0-9]+) ([0-9]+) (.*)$'

    def line_to_request(self, line: str) -> request:
        """
        The way our logs are formatted requires an additional part.

        Our logs begin with host, which the driver does not consider by
        default - and obviously, we can't assume other logs match our format.

        i.e. this may need to be configurable...
        """
        parts = line.split(" ", 4)
        try:
            _, ip_address, _, _, rest = parts
            matches, timestamp = self.check_url_matches(rest, line)
        except ValueError as err:
            raise ValueError(
                "Your file has not the right structure, ", err
            )
        request = matches.group(1).strip('"')
        response_code = int(matches.group(2))
        content_length = int(matches.group(3))
        referer_and_ua = matches.group(4)
        matches = self.r_n_ua_re.match(referer_and_ua)
        if matches:
            referer = matches.group(1)
            user_agent = matches.group(2)
        else:
            raise ValueError(
                "There wasn't any match with the url"
            )
        if len(request.split()) >= 3:
            # version is unused, also ignore request if it's missing stuff
            method, url, _ = request.split()
        else:
            return
        return Request(
            ip_address,
            timestamp,
            method,
            url,
            response_code,
            content_length,
            referer,
            user_agent,
        )

    def check_url_matches(self, rest, line):
        """Check the url has got the right structure
        and it matches the request regex"""
        if len(rest) < 30:
            print(f">>{line}<<")
        timestamp = rest[1:27]
        if not rest[29] == '"':
            raise ValueError("Request column malformed")
        last_five = rest[30:]
        matches = self.request_re.match(last_five)
        if not matches:
            matches = re.compile(self.fallback_re).match(last_five)
        return matches, timestamp

    def unzip(self, filename: str) -> subprocess:
        proc = subprocess.Popen(["zcat", filename], stdout=subprocess.PIPE)
        output = proc.communicate()[0]
        return output

    def logfile_names(self) -> Iterator[str]:
        for path in sorted(os.listdir(self.log_dir)):
            """
            Generate a list of matching logfile names in the directory
            Note - can't assume logs start with 'access.log' - e.g. our log
            names have the format <service>_<code>_access.log-<datestamp>.gz
            """
            if 'access.log' not in path or not path.endswith(".gz"):
                continue

            """The timestamp in our logs also don't include a '-'
            i.e. they would end like this: access.log-20230602.gz
            """
            match_pattern = re.compile(
                r'(?P<year>\d{4})-?(?P<month>\d{2})-?(?P<day>\d{2})'
            )
            match = match_pattern.search(path)
            if match is None:
                raise AttributeError(
                    "Your file has to have a date at the end of it's name"
                )
            date_dict = match.groupdict()
            timestamp = (
                f"{date_dict['year']}-{date_dict['month']}-{date_dict['day']}"
            )
            try:
                time.strptime(timestamp, '%Y-%m-%d')
            except ValueError:
                continue

            yield os.path.join(self.log_dir, path)

    def lines(self) -> Iterator[str]:
        """Generate a stream of lines from the zipped log files."""
        for logfile in self.logfile_names():
            data = self.unzip(logfile)
            for line in data.splitlines():
                yield line

    def relevant_requests(self) -> Iterator[tuple]:
        """Generate a filtered stream of requests; apply the predicate list
           `self.filters' to these requests; if any predicate fails, ignore
           the request and do not generate it for downstream processing."""
        for line in self.lines():
            if line_request := self.line_to_request(line.decode('utf-8')):
                for filter_group in self.filter_groups:
                    ok = True
                    stream, filters, regex = filter_group
                    for f in filters:
                        if not f(line_request):
                            ok = False
                            break
                    if not ok:
                        continue
                    line_request.sanitise_url(regex)
                    yield (stream, line_request)

    def __iter__(self):
        for i in self.relevant_requests():
            yield i
        return

    def to_csvs(self) -> None:
        """filters, regex are not used in self.filter_groups."""
        streams = [stream for stream, _, _ in self.filter_groups]
        csv_writers = {}
        for stream in streams:
            w = csv.writer(stream)
            csv_writers[stream] = w
        for stream, req in self:
            w = csv_writers[stream]
            w.writerow(req.as_tuple())
