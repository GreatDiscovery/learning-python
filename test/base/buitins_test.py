import unittest


class TestBuiltIns(unittest.TestCase):
    # 把多个可迭代对象按位置打包在一起。
    def testZip(self):
        a = [1, 2, 3]
        b = ['a', 'b', 'c']
        l = list(zip(a, b))
        print(l)
        assert l == [(1, 'a'), (2, 'b'), (3, 'c')]

        for l, x in zip(a, b):
            print(l)
            print(x)

        c = list(zip(a, (4, 5, 6)))
        print(c)



