import unittest


class TestForExample(unittest.TestCase):
    def test_for_i(self):
        arr = [1, 2, 3, 4, 5]
        for index in range(len(arr)):
            print(arr[index])

    def test_for_i_2(self):
        for i in range(0, 16384):
            print(i)

    def test_join_comma(self):
        # 创建一个列表
        list_ = ['apple', 'banana', 'cherry']
        # 使用join()方法将列表中的元素用逗号拼接起来
        string = ','.join(list_)
        print(string)  # 输出：apple, banana, cherry
