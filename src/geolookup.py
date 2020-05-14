"""
Look up the country associated with an IP address at a particular time.
"""


from sqlite3 import dbapi2 as sqlite


class GeoLookup(object):
    def __init__(self, path):
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

    def lookup_country(self, ip_address, date):
        if self.db is None:
            return ''
        time_max = date.timestamp()
        time_min = time_max - 86400 * 180
        self.cursor.execute(self.query, (ip_address, time_min, time_max))
        row = self.cursor.fetchone()
        if row is None:
            return ''
        else:
            return self.prefix + row[0]
