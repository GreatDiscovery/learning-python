import unittest

x = 10  # 这是一个全局变量

def modify_global_variables():
    global y # 当你需要在函数内部修改一个全局变量时，必须使用 global 关键字。
    y = 10
    x = 20


class MyTestCase(unittest.TestCase):
    def test_global(self):
        y = 20
        assert y == 20
        assert x == 10

if __name__ == '__main__':
    unittest.main()
