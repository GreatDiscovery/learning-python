import unittest


class TestForExample(unittest.TestCase):
    def test_for_i(self):
        arr = [1, 2, 3, 4, 5]
        for index in range(len(arr)):
            print(arr[index])

