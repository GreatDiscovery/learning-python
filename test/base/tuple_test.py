import unittest


# tuple 是：一个不可修改的有序元素集合，用逗号定义。
class TestTuple(unittest.TestCase):
    def test_tuple(self):
        a = (1, 2, 3)
        # Tuples don't support item assignment
        # a[0] = 10

        a = 1, 2, 3
        assert type(a) == tuple

        # This is not a tuple
        a = (5)
        assert type(a) == int
