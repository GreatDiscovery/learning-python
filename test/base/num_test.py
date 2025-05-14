import unittest


class TestNumExample(unittest.TestCase):
    def testFloat(self):
        assert round(3.455, 2) == 3.46
        assert round(3.454, 2) == 3.45
        assert round(100 / 100, 2) == 1
        assert round(0 / 100, 2) == 0
        assert round(1 / 100, 2) == 0.01
        assert round(50 / 100, 2) == 0.5
