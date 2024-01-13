import unittest


class TestDictExample(unittest.TestCase):
    def test_init_dict(self):
        tinydict = {'Name': 'Zara', 'Age': 7, 'Class': 'First'}

        tinydict['Age'] = 8  # 更新
        tinydict['School'] = "RUNOOB"  # 添加

        print("tinydict['Age']: ", tinydict['Age'])
        print("tinydict['School']: ", tinydict['School'])
        print(len(tinydict))

    def test_list(self):
        arr = [5, 3, 4, 1, 2]
        for i in arr:
            print(i)

    def test_range(self):
        d1 = {1: 1, 2: 2, 3: 3, 4: 4}
        for k in d1:
            print(f'k={k}')
