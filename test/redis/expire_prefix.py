import redis
import concurrent.futures
from typing import List
import argparse
import crc16

"""
    用于批量删除前缀key
"""


class RedisKeyDeleter:
    def __init__(self, redis_ips: List[str], prefix: str, max_threads: int = 10, scan_count: int = 1000,
                 pipeline_count: int = 1000, dry_run: bool = True, slots: int = 4095):
        """
        初始化 RedisKeyDeleter

        :param redis_ips: Redis 节点 IP 列表
        :param prefix: 需要删除的键的前缀
        :param max_threads: 最大线程数，默认值为 10
        :param dry_run: 如果空跑的话，只打印不删除
        :param scan_count: scan扫描的batch
        :param pipeline_count: 发送删除命令的
        """
        self.dry_run = dry_run
        self.redis_ips = redis_ips
        self.prefix = prefix
        self.max_threads = max_threads
        self.scan_count = scan_count
        self.pipeline_count = pipeline_count
        self.slots = slots

    def _delete_keys_with_prefix(self, redis_client: redis.StrictRedis):
        """
        从 Redis 节点中删除匹配指定前缀的所有键

        :param redis_client: Redis 客户端实例
        """
        slot_key_map = {}
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor, match=self.prefix + '*', count=self.scan_count)
            if keys:
                for key in keys:
                    if self.dry_run is True:
                        print(f"dry run deleted key: {key}")
                    else:
                        print(f"Deleted {len(keys)} keys from {redis_client.connection_pool.connection_kwargs['host']}")
                        slot = self.redis_crc16(key)
                        if slot not in slot_key_map:
                            slot_key_map[slot] = []
                        slot_key_map[slot].append(key)
                        if slot_key_map[slot] and len(slot_key_map[slot]) >= self.pipeline_count:
                            for k in slot_key_map[slot]:
                                print(f'key: {k}')
                                pipeline = redis_client.pipeline()
                                pipeline.delete(k)
                                pipeline.execute()
                            slot_key_map[slot] = []
            if cursor == 0:
                break
        # 补发剩余部分
        if self.dry_run is False:
            for slot in slot_key_map:
                for key in slot_key_map[slot]:
                    pipeline = redis_client.pipeline()
                    pipeline.delete(key)
                    pipeline.execute()
            slot_key_map.clear()

    def _delete_keys_from_redis_node(self, ip: str):
        """
        连接到 Redis 节点并删除匹配指定前缀的所有键

        :param ip: Redis 节点的 IP 地址
        """
        r = redis.StrictRedis(host=ip, port=6379, db=0)
        self._delete_keys_with_prefix(r)
        r.close()

    def delete_keys(self):
        """
        在所有 Redis 节点上并发删除匹配指定前缀的所有键，限制最大线程数
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._delete_keys_from_redis_node, ip) for ip in self.redis_ips]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # 等待每个任务完成

        print("Finished deleting keys from all Redis nodes.")

    def redis_crc16(self, raw_key):
        # Redis的CRC16算法是基于CCITT标准的CRC16算法的变种
        # 该变种使用大端字节序 (big-endian)
        crc = crc16.crc16xmodem(raw_key) & self.slots
        return crc

    def slot_key_map(self, keys):
        slot_dict = {}
        for key in keys:
            slot = self.redis_crc16(key)
            if slot not in slot_dict:
                slot_dict[slot] = []
            slot_dict[slot].append(key)
        return slot_dict


# 示例用法
# --dry-run False才会真正执行删除数据，否则只会打印待删除的数据
# python3 ./expire_prefix.py --dry-run True --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --max-threads 10 --scan-count 1000 --pipeline-count 1000 --slots 4095 > audit.log
# python3 ./expire_prefix.py --dry-run False --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --max-threads 10 --scan-count 1000 --pipeline-count 1000 --slots 4095 > audit.log

if __name__ == "__main__":
    # 使用 argparse 获取命令行参数
    parser = argparse.ArgumentParser(description="Delete Redis keys with a specified prefix.")
    parser.add_argument('--dry-run', type=str, default=True, required=True, help='prints keys but do not delete keys')
    parser.add_argument('--redis-ips', type=str, required=True, help='Comma-separated list of Redis IPs')
    parser.add_argument('--prefix', type=str, required=True, help='Prefix of the Redis keys to delete')
    parser.add_argument('--max-threads', type=int, default=10, help='Maximum number of threads to use (default is 10)')
    parser.add_argument('--scan-count', type=int, default=1000, help='Maximum number of scan to use (default is 1000)')
    parser.add_argument('--pipeline-count', type=int, default=1000,
                        help='Maximum number of scan to use (default is 1000)')
    parser.add_argument('--slots', type=int, default=4095, help='redis cluster slots')

    args = parser.parse_args()

    # 将 IP 字符串转换为列表
    redis_ips = args.redis_ips.split(',')
    dry_run = True
    if args.dry_run == 'False' or args.dry_run == 'false':
        dry_run = False
    prefix = args.prefix
    max_threads = args.max_threads  # 最大线程数
    scan_count = args.scan_count
    pipeline_count = args.pipeline_count
    slots = args.slots

    if prefix == "" or prefix is None:
        print("prefix cannot be empty")
        exit(1)
    # 创建 RedisKeyDeleter 实例并删除前缀键
    print(
        f'redis ips: {redis_ips}, prefix: {prefix}, max_threads: {max_threads}, scan_count: {scan_count}, pipeline_count: {pipeline_count}, dry_run: {dry_run}, slots: {slots}')
    deleter = RedisKeyDeleter(redis_ips, prefix, max_threads, scan_count, pipeline_count, dry_run, slots)
    deleter.delete_keys()
