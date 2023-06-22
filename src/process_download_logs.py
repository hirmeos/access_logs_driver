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
from typing import BinaryIO
from requests import request

from logdata import LogStream


def get_spiders(spiders):
    """ Import the list of user agent strings identifying known web crawlers,
    bots, spiders, etc

    Args:
        spiders (_type_): _description_
    """
    path = os.path.join(os.path.dirname(__file__), 'spiders')
    f = open(path, 'r', encoding='ISO-8859-1')
    [spiders.add(line.strip("\n")) for line in f.readlines()]


def only_successful(request: request) -> request:
    return request.response_code in [200, 304]


def nostar(request: request) -> str:
    return request.url != "*"


def method_ok(request: request) -> str:
    return request.method == "GET" or request.method == "POST"


def no_plus_http(request):
    return "+http" not in request.user_agent


def make_filters(regexes: re) -> list:
    spiders = set()
    get_spiders(spiders)
    excluded = json.loads(os.getenv('EXCLUDED_IPS'))

    def not_known_spider(request):
        return request.user_agent not in spiders

    def not_excluded_ip(request):
        return request.ip_address not in excluded

    def filter_url(request):
        for regex in regexes:
            if re.search(re.compile(regex), request.url) is not None:
                return True
        return False

    return [
        filter_url,
        only_successful,
        nostar,
        method_ok,
        no_plus_http,
        not_known_spider,
        not_excluded_ip
    ]


def output_stream(filename: str) -> BinaryIO:
    return open(filename, "w")


def get_output_filename(odir, name) -> str:
    return f"{odir}/output_{name}.csv"


def run():
    modes = json.loads(os.getenv('MODES'))
    logdir = os.environ['LOGDIR']
    odir = os.environ['CACHEDIR']

    filter_groups = []
    for m in modes:
        filename = get_output_filename(odir, m['name'])
        filters = (
            output_stream(filename),
            make_filters(m['regex']),
            m['regex']
        )
        filter_groups.append(filters)

    logs = LogStream(logdir, filter_groups)
    logs.to_csvs()


if __name__ == '__main__':
    run()
