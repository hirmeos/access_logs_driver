import sqlite3
import sys
from unittest import TestCase


class DB:
    def __init__(self, dbname='mydb.db'):
        try:
            self.connection = sqlite3.connect(dbname)
        except RuntimeError as err:
            print('Error connecting to the dB ', err)
        finally:
            pass


class TestGeolookup(TestCase):

    def setUp(self):
        self.db = DB('test.db')
        self.ip_address, self.timestamp = sys.argv

    def test_lookup_country(self):
        pass
