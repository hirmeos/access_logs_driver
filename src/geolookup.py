#!/usr/bin/env python3
"""
Look up the country associated with an IP address at a particular time.
"""


import sys
import time
import datetime
from sqlite3 import dbapi2 as sqlite


class GeoLookup(object):
    def __init__(self, path: str) -> None:
        if path is None:
            self.db = None
            return
        self.db = sqlite.connect(path)
        self.cursor = self.db.cursor()
        self.cache = {}
        self.query = '''
          SELECT country, datestamp FROM ipgeo
            WHERE ip_address = ?
            AND datestamp >= ?
            AND datestamp <= ?;
        '''
        self.prefix = 'urn:iso:std:3166:-2:'

    def lookup_country(self, ip_address: str, date: datetime) -> str:
        if self.db is None:
            return ''
        time_now = date.timestamp()
        time_max = time_now + 86400 * 180
        time_min = time_max - 86400 * 180
        args = (ip_address, time_min, time_max)
        self.cursor.execute(self.query, args)
        row = self.cursor.fetchone()
        if row is None:
            return ''
        return self.prefix + row[0]


def run() -> None:
    _, ip_address, timestamp = sys.argv
    ts = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    ds = datetime.datetime(*ts[:6])
    geo = GeoLookup("/logs/ip_addresses.db")
    print(geo.lookup_country(ip_address, ds))


if __name__ == '__main__':
    run()
