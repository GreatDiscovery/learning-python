import time
import unittest
from datetime import datetime, timedelta


class TestDateExample(unittest.TestCase):
    def testNow(self):
        print(datetime.now())

    def testDateDelta(self):
        # 输入2个日期
        date1 = "2022.05.11 13:30:00"
        date2 = "2022.05.10 12:00:00"

        # 将输入的日期转换为“datetime.datetime”类型
        # 由于日期的类型是字符串，因此不能直接进行计算，会报错
        date1 = datetime.strptime(date1, "%Y.%m.%d %H:%M:%S")
        date2 = datetime.strptime(date2, "%Y.%m.%d %H:%M:%S")
        print(" date1:", date1, "\n", "date2:", date2)
        print(" 2个日期的类型分别是:\n", type(date1), type(date2))

        duration = date1 - date2
        day = duration.days
        hour = duration.seconds / 3600
        print("days:", day)
        print("hours:", hour)

        date3 = date1 + timedelta(days=1)
        print(date3)

        # 打印时间戳
        print(int(time.time()))