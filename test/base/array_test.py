import unittest
import math

class TestArrayExample(unittest.TestCase):
    def testArrRange(self):
        arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for i in range(0, len(arr), 2):
            print(arr[i:i+2])
            for j in arr[i:i+2]:
               print(j)