import unittest
from datetime import datetime


class TestDateExample(unittest.TestCase):
    def testNow(self):
        print(datetime.now())
