import unittest
import crc16


def redis_crc16(key):
    # Redis的CRC16算法是基于CCITT标准的CRC16算法的变种
    # 该变种使用大端字节序 (big-endian)
    crc = crc16.crc16xmodem(key.encode('utf-8')) & 0x3FFF
    return crc


def get_redis_slot(key):
    slot = redis_crc16(key)
    return slot


class TestSlotExample(unittest.TestCase):
    def test_get_slot(self):
        key = "k1"
        slot = get_redis_slot(key)
        print(f'slot={slot}')
        assert slot == 12706
