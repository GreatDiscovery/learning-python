import unittest


class TestStrExample(unittest.TestCase):
    def testPrint(self):
        name = "hello"
        age = 15
        print(f"name={name}, age={age}")
