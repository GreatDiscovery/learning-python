import unittest


class TestStrExample(unittest.TestCase):
    def testPrint(self):
        name = "hello"
        age = 15
        print(f"name={name}, age={age}")

    def testFormat(self):
        run_info = "{\"run_time_info\": {\"cluster_name\": \"fls-review-1/,/fls-review-1\"}}"
        print(run_info)
        run_info = "{\"run_time_info\": {\"cluster_name\": \"%s-%s/,/%s-%s\"}}" % ("fls-review", 1, "fls-review", 1)
        print(run_info)
