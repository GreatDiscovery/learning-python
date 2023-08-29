import unittest

# 参考：https://zhuanlan.zhihu.com/p/45535784
class TestDbExample(unittest.TestCase):
    def test_example1(self):
        # 测试例子1
        self.assertEqual(1, 1)
        self.assertEqual(2, 2)

    def test_example2(self):
        # 测试例子2
        self.assertEqual(3, 3)
        self.assertEqual(4, 4)


if __name__ == '__main__':
    unittest.main()
