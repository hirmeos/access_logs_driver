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

import io
import json
import os
import re

from logdata import LogStream


def get_spiders(spiders: set) -> None:
    """ Import the list of user agent strings identifying known web crawlers,
    bots, spiders, etc

    Args:
        spiders (_type_): _description_
    """
    path = os.path.join(os.path.dirname(__file__), 'spiders')
    with open(path, 'r', encoding='ISO-8859-1') as file:
        [spiders.add(line.strip("\n")) for line in file.readlines()]


def only_successful(request: str) -> str:
    return request.response_code in [200, 304]


def nostar(request: str) -> str:
    return request.url != "*"


def method_ok(request: str) -> str:
    return request.method == "GET" or request.method == "POST"


def no_plus_http(request: str) -> str:
    return "+http" not in request.user_agent


def make_filters(regexes: re, excluded: str) -> list:
    spiders = set()
    get_spiders(spiders)

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


def output_stream(filename: str) -> io.TextIOWrapper:
    return open(filename, "w")


def get_output_filename(odir, name) -> str:
    return f"{odir}/output_{name}.csv"


def run() -> None:
    """
    Entry point before executing the file logdata and process the
    content of the .gz files and finally write an output file.
    """
    modes = json.loads(os.getenv('MODES'))
    logdir = os.environ.get('LOGDIR')
    odir = os.environ.get('CACHEDIR')
    excluded = json.loads(os.getenv('EXCLUDED_IPS'))
    url_prefix = os.environ.get('URL_PREFIX')

    filter_groups = []
    for m in modes:
        filename = get_output_filename(odir, m['name'])
        filters = (
            output_stream(filename),
            make_filters(m['regex'], excluded),
            m['regex']
        )
        filter_groups.append(filters)

    logs = LogStream(logdir, filter_groups, url_prefix)
    logs.to_csvs()


if __name__ == '__main__':
    run()
