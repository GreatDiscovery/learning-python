import unittest


class TestDictExample(unittest.TestCase):
    def test_init_dict(self):
        tinydict = {'Name': 'Zara', 'Age': 7, 'Class': 'First'}

        tinydict['Age'] = 8  # 更新
        tinydict['School'] = "RUNOOB"  # 添加

        print("tinydict['Age']: ", tinydict['Age'])
        print("tinydict['School']: ", tinydict['School'])
        print(len(tinydict))
