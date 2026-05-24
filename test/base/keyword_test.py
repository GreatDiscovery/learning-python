import unittest

x = 10  # 这是一个全局变量


def modify_global_variables():
    global y  # 当你需要在函数内部修改一个全局变量时，必须使用 global 关键字。
    y = 10
    x = 20


class MyTestCase(unittest.TestCase):
    def test_global(self):
        y = 20
        assert y == 20
        assert x == 10

    def test_yield(self):
        """"
            yield vs return 对比表
            特性	return	yield
            函数状态	函数结束，状态丢失	函数暂停，状态保留
            返回值次数	1次	多次
            内存使用	一次性返回所有结果	逐个产生结果，省内存
            能否继续	不能	可以继续执行
            适用场景	计算完就有结果	需要逐步产生结果，或处理大数据
        """

        # 普通函数：一次性执行完，返回一个值
        def normal_function():
            result = 1 + 2
            return result  # 返回结果，函数结束

        print(normal_function())  # 输出: 3

        # 使用yield的函数：可以暂停，多次返回值
        def generator_function():
            yield 1  # 暂停在这里，返回1
            yield 2  # 恢复执行，再暂停，返回2
            yield 3  # 再恢复，再暂停，返回3

        gen = generator_function()
        print(next(gen))  # 输出: 1 (第一次恢复，拿到1)
        print(next(gen))  # 输出: 2 (再次恢复，拿到2)
        print(next(gen))  # 输出: 3 (再次恢复，拿到3)

    def test_yield_from(self):
        # 不用 yield from 的写法
        def generator_without_yield_from():
            for item in [1, 2, 3]:
                yield item

        # 使用 yield from 的写法
        def generator_with_yield_from():
            yield from [1, 2, 3]

        # 测试
        print(list(generator_without_yield_from()))  # 输出: [1, 2, 3]
        print(list(generator_with_yield_from()))  # 输出: [1, 2, 3]

    def test_yield_from_return(self):
        """关键点：yield from 会捕获子生成器的 return 值！"""

        def sub_generator():
            yield 1
            yield 2
            return "完成啦！"  # 注意：return 而不是 yield

        def main_generator():
            # result 会接收子生成器的 return 值
            result = yield from sub_generator()
            print(f"子生成器返回: {result}")
            yield 3

        # 使用
        for value in main_generator():
            print(value)

        # 输出：
        # 1
        # 2
        # 子生成器返回: 完成啦！
        # 3


if __name__ == '__main__':
    unittest.main()
