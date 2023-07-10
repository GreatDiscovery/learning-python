from unittest import TestCase

from db.mysql_pool import MySQLConnectionPool


class TestMySQLConnectionPool(TestCase):
    def test_fetchone(self):
        cfg = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'passwd': '123456',
            'db': 'test'
        }

        db = MySQLConnectionPool(**cfg)
        table_name = 'user'
        sql = f"select count(*) total from {table_name}"
        result = db.fetchone(sql)
        number = result.get('total')
        print(result)
        print(number, type(number))
        self.assertEqual(number, 1)
