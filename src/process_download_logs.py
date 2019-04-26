#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Iterate through a bunch of gzipped Apache HTTP log files
Exclude bots, scrapers, etc

Select URLs matching the provided regex(es)

Generate a CSV of the relevant log entries thus:

Output is as a CSV of 4-tuples of type
 (timestamp * IP address * URL * user agent)
"""

import re
import os
import json
from logdata import LogStream


# Import the list of user agent strings identifying known web crawlers,
# bots, spiders, etc
def get_spiders(spiders):
    path = os.path.join(os.path.dirname(__file__), 'spiders')
    f = open(path, 'r', encoding='ISO-8859-1')
    [spiders.add(line.strip("\n")) for line in f.readlines()]


def only_successful(r):
    return r.response_code in [200, 304]


def nostar(r):
    return r.url != "*"


def method_ok(r):
    return r.method == "GET" or r.method == "POST"


def no_plus_http(r):
    return "+http" not in r.user_agent


def make_filters(regexes):
    spiders = set()
    get_spiders(spiders)
    excluded = json.loads(os.getenv('EXCLUDED_IPS'))

    def not_known_spider(r):
        return r.user_agent not in spiders

    def not_excluded_ip(r):
        return r.ip_address not in excluded

    def filter_url(r):
        for regex in regexes:
            if re.search(re.compile(regex), r.url) is not None:
                return True
        return False

    return [filter_url, only_successful, nostar, method_ok, no_plus_http,
            not_known_spider, not_excluded_ip]


def output_stream(filename):
    return open(filename, "w")


def get_output_filename(odir, name):
    return "%s/output_%s.csv" % (odir, name)


def run():
    modes = json.loads(os.getenv('MODES'))
    logdir = os.environ['LOGDIR']
    odir = os.environ['CACHEDIR']

    filter_groups = []
    for m in modes:
        filename = get_output_filename(odir, m['name'])
        filters = (output_stream(filename),
                   make_filters(m['regex']),
                   m['regex'])
        filter_groups.append(filters)

    logs = LogStream(logdir, filter_groups)
    logs.to_csvs()


if __name__ == '__main__':
    run()
