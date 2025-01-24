import json
import unittest


class TestStrExample(unittest.TestCase):
    def testChinese(self):
        name = "宋"
        assert len(name.encode("GBK")) == 2
        assert len(name.encode("utf-8")) == 3

        name = "a宋"
        assert len(name.encode("GBK")) == 3
        assert len(name.encode("utf-8")) == 4


    def testContain(self):
        name = "hello world"
        assert "hello" in name
        assert "world" in name
        assert " " in name
        assert "a" not in name

    def testPrint(self):
        name = "hello"
        age = 15
        print(f"name={name}, age={age}")

    def testFormat(self):
        run_info = "{\"run_time_info\": {\"cluster_name\": \"fls-review-1/,/fls-review-1\"}}"
        print(run_info)
        run_info = "{\"run_time_info\": {\"cluster_name\": \"%s-%s/,/%s-%s\"}}" % ("fls-review", 1, "fls-review", 1)
        print(run_info)
        value = "{\"cluster_name\": \"%s-%s/,/%s-%s\"}" % ("fls-review", 1, "fls-review", 1)
        payload = {'run_time_info': value}
        print(json.dumps(payload))
        source_cluster = "fls-review"
        source_version = 1
        dest_cluster = "fls-review"
        dest_version = 1
        str1 = f"{source_cluster}-{source_version}/,/{dest_cluster}-{dest_version}"
        map_value = {"cluster_name": str1}
        payload = {"run_time_info": map_value}
        print(json.dumps(payload))
        cluster_dict = {"cluster_name": f"{source_cluster}-{source_version}"}
        print(cluster_dict)

    def testStrNoneOrEmpty(self):
        str1 = None
        str2 = ""
        if str1:
            print(len(str1))
        print(len(str2))

    def testTrimFun(self):
        str1 = " hello world "
        print(str.strip(str1))
        str2 = ""
        print(str.strip(str2))
