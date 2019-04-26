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
import urllib.error
import urllib.parse


URL_PREFIX = os.environ['URL_PREFIX']


class Request(object):
    """Represent the data in a single line of the Apache log file."""

    def __init__(self, ip_address, timestamp, method, url, response_code,
                 content_length, referer, user_agent, valid):
        assert response_code >= 100
        assert response_code < 1000
        assert content_length >= 0
        self.ip_address = ip_address
        self.timestamp = time.strptime(timestamp[:20], '%d/%b/%Y:%H:%M:%S')
        self.method = method
        self.url = self.normalise_url(self.convert_url(url))
        self.response_code = response_code
        self.content_length = content_length
        self.referer = referer
        self.user_agent = user_agent
        self.valid = valid

    @staticmethod
    def normalise_url(url):
        try:
            u = URL_PREFIX + url.lower()
            return u[:-1] if u[-1] == "/" else u
        except BaseException:
            print("Error parsing: " + url, file=sys.stderr)
            raise

    @staticmethod
    def convert_url(url):
        try:
            if url.startswith("http"):
                u = urllib.parse.urlparse(url).path
            else:
                u = url
            return re.sub(r'^//', '/', re.sub(r'([^:])/+', '\\1/', u))
        except BaseException:
            print("Error parsing: " + url, file=sys.stderr)
            raise

    def fmttime(self):
        fmt = "%Y-%m-%d %H:%M:%S"
        return datetime.datetime(*self.timestamp[:6]).strftime(fmt)

    def __str__(self):
        return "Request(%s, %s, %s)" % (self.fmttime(), self.ip_address,
                                        self.url)

    def as_tuple(self):
        return (self.fmttime(), self.ip_address, self.url, self.user_agent)

    def sanitise_url(self, regexes):
        for regex in regexes:
            matched = re.search(re.compile(regex), self.url)
            if matched is not None:
                self.url = matched.group(0)
                break


class LogStream(object):
    def __init__(self, log_dir, filter_groups):
        self.log_dir = log_dir
        self.filter_groups = filter_groups

    request_re = re.compile(r'^(.*[^\\]") ([0-9]+) ([0-9]+) (.*)$')
    r_n_ua_re = re.compile(r'^"(.*)" "(.*)" *$')
    fallback_re = r'^()" ([0-9]+) ([0-9]+) (.*)$'

    # Generate Request objects for each line in the input stream
    def line_to_request(self, line):
        parts = line.split(" ", 3)
        ip_address, user1, user2, rest = parts
        if len(rest) < 30:
            print(">>%s<<" % line)
        assert rest[28] == " ", line
        timestamp = rest[1:27]

        assert rest[29] == '"'
        last_five = rest[30:]

        matches = self.request_re.match(last_five)
        if matches is None:
            if last_five.startswith('" '):
                matches = re.compile(self.fallback_re).match(last_five)
            else:
                print(last_five)
                raise
        request = matches.group(1).strip('"')
        response_code = int(matches.group(2))
        content_length = int(matches.group(3))
        referer_and_ua = matches.group(4)

        matches = self.r_n_ua_re.match(referer_and_ua)
        try:
            referer = matches.group(1)
            user_agent = matches.group(2)
        except BaseException:
            print(referer_and_ua)
            raise

        valid = True
        try:
            method, url, version = request.split()
        except ValueError:
            method = None
            url = ""
            valid = False

        return Request(ip_address, timestamp, method, url, response_code,
                       content_length, referer, user_agent, valid)

    def unzip(self, filename):
        proc = subprocess.Popen(["zcat", filename], stdout=subprocess.PIPE)
        output = proc.communicate()[0]
        return output

    # Generate a list of matching logfile names in the directory
    def logfile_names(self):
        for path in sorted(os.listdir(self.log_dir)):
            if not path.startswith("access.log"):
                continue
            if not path.endswith(".gz"):
                continue
            match = re.search(r'\d{4}-\d{2}-\d{2}', path)
            timestamp = match.group()
            try:
                time.strptime(timestamp, '%Y-%m-%d')
            except ValueError:
                continue

            yield os.path.join(self.log_dir, path)

    # Generate a stream of lines from the zipped log files
    def lines(self):
        for logfile in self.logfile_names():
            data = self.unzip(logfile)
            for line in data.splitlines():
                yield line

    def relevant_requests(self):
        """Generate a filtered stream of requests; apply the predicate list
           `self.filters' to these requests; if any predicate fails, ignore
           the request and do not generate it for downstream processing"""
        for line in self.lines():
            i = self.line_to_request(line.decode('utf-8'))
            if not i.valid:
                continue
            for filter_group in self.filter_groups:
                ok = True
                stream, filters, regex = filter_group
                for f in filters:
                    if not f(i):
                        ok = False
                        break
                if not ok:
                    continue
                i.sanitise_url(regex)
                yield (stream, i)

    def __iter__(self):
        for i in self.relevant_requests():
            yield i
        raise StopIteration

    def to_csvs(self):
        streams = [stream for stream, filters, regex in self.filter_groups]
        csv_writers = {}
        for stream in streams:
            w = csv.writer(stream)
            csv_writers[stream] = w
        for stream, req in self:
            w = csv_writers[stream]
            w.writerow(req.as_tuple())
