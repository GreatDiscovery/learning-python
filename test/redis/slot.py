import re
import unittest
import crc16


def redis_crc16(raw_key):
    # Redis的CRC16算法是基于CCITT标准的CRC16算法的变种
    # 该变种使用大端字节序 (big-endian)
    crc = crc16.crc16xmodem(raw_key.encode('utf-8')) & 4095
    return crc


def extract_braces_content(input_string):
    # 使用正则表达式查找大括号内的内容
    matches = re.findall(r'\{(.*?)\}', input_string)
    return matches


def get_redis_slot(raw_key):
    # has hash tag
    if "{" in raw_key and "}" in raw_key:
        raw_key_list = extract_braces_content(raw_key)
        if len(raw_key_list) > 0:
            raw_key = raw_key_list[0]
    slot = redis_crc16(raw_key)
    return slot


# class TestSlotExample(unittest.TestCase):
#     def test_get_slot(self):
#         key = b"4test5555_8_2028_801"
#         slot = get_redis_slot(key)
#         print(f'slot={slot}')
#
#     def test_hash_tag(self):
#         key1 = b'2024-01-13 10:30:02.459 stat_slot_prefix_key{71861}'
#         slot = get_redis_slot(str(key1))
#         print(f'slot={slot}')
#         assert slot == 5154


if __name__ == '__main__':
    key1 = "key:000000616373"
    slot = get_redis_slot(key1)
    print(f'slot={slot}')

    key2 = b"key:000000616373"
    slot = get_redis_slot(key2.decode('utf-8'))
    print(f'slot={slot}')
