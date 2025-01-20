import redis
import concurrent.futures
from typing import List


class RedisKeyDeleter:
    def __init__(self, redis_ips: List[str], prefix: str, max_threads: int = 10, scan_count: int = 1000, dry_run: bool = True):
        """
        初始化 RedisKeyDeleter

        :param redis_ips: Redis 节点 IP 列表
        :param prefix: 需要删除的键的前缀
        :param max_threads: 最大线程数，默认值为 10
        :param dry_run: 如果空跑的话，只打印不删除
        """
        self.dry_run = dry_run
        self.redis_ips = redis_ips
        self.prefix = prefix
        self.max_threads = max_threads
        self.scan_count = scan_count

    def _delete_keys_with_prefix(self, redis_client: redis.StrictRedis):
        """
        从 Redis 节点中删除匹配指定前缀的所有键

        :param redis_client: Redis 客户端实例
        """
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor, match=self.prefix + '*', count=self.scan_count)
            if keys:
                print(f"Deleted {len(keys)} keys from {redis_client.connection_pool.connection_kwargs['host']}")
                for key in keys:
                    if self.dry_run:
                        print(f"dry run Deleted {key}")
                    else:
                        pipeline = redis_client.pipeline()
                        pipeline.delete(key)
                        pipeline.execute()
            if cursor == 0:
                break

    def _delete_keys_from_redis_node(self, ip: str):
        """
        连接到 Redis 节点并删除匹配指定前缀的所有键

        :param ip: Redis 节点的 IP 地址
        """
        r = redis.StrictRedis(host=ip, port=6379, db=0)
        self._delete_keys_with_prefix(r)

    def delete_keys(self):
        """
        在所有 Redis 节点上并发删除匹配指定前缀的所有键，限制最大线程数
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._delete_keys_from_redis_node, ip) for ip in self.redis_ips]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # 等待每个任务完成

        print("Finished deleting keys from all Redis nodes.")


# 示例用法
if __name__ == "__main__":
    dry_run = True
    # Redis 节点 IP 地址和前缀
    redis_ips = ['10.74.110.58', '10.74.40.101', '10.74.204.2']
    prefix = "key:"
    max_threads = 5  # 最大线程数
    scan_count = 1000

    if prefix == "" or prefix is None:
        print("prefix cannot be empty")
        exit(1)
    # 创建 RedisKeyDeleter 实例并删除前缀键
    deleter = RedisKeyDeleter(redis_ips, prefix, max_threads, scan_count, dry_run)
    deleter.delete_keys()
