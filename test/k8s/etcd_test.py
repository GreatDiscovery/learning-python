import unittest
import etcd3

# reference https://github.com/kubernetes-client/python
class TestEtcdExample(unittest.TestCase):
    def test_get(self):
        host = "10.4.47.0"
        port = 2379
        client = etcd3.client(host=host, port=port)
        value, meta = client.get("k1")
        print(value)

    def test_txn(self):
        host="10.4.47.0"
        port=2379
        client = etcd3.client(host=host, port=port)
        # 构建事务条件
        compare = [
            # client.transactions.value('key1') == "",
        ]

        # 构建成功操作
        success = [
            client.transactions.delete('key1'),  # 删除 key1
            client.transactions.delete('key2'),  # 删除 key2
            client.transactions.delete('key3')  # 删除 key3

            # client.transactions.put('key1', 'value1'),
            # client.transactions.put('key2', 'value2'),
            # client.transactions.put('key3', 'value3')
        ]

        # 失败操作（这里为空，可以根据需要定义）
        failure = []

        # 执行事务
        success = client.transaction(
            compare=compare,
            success=success,
            failure=failure
        )

        if success:
            print("Transaction succeeded: Keys put.")
        else:
            print("Transaction failed: Conditions were not met.")

