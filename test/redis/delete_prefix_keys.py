import signal
import sys
import threading
import time
import redis
import concurrent.futures
from typing import List
import argparse
from redis.connection import ConnectionPool
from redis.exceptions import RedisError, ResponseError
from queue import Queue
from threading import Lock

"""

    fix： 废弃，不好用
    用于批量删除前缀key
    示例用法，注意最好重定向到audit.log文件里，记录删除的key。在实际使用中，先用--dry-run True空跑，把所有要删除的key打印出来检查一遍，同时观察scan操作对线上的影响，如果符合预期，再用--dry-run False执行真正的删除
    --dry-run False才会真正执行删除数据，True只会打印待删除的数据
    --redis-ips master ip，使用逗号分隔
    --prefix 填确定的字符，不要包含'*'号
    --max-threads 并发线程数，建议设置为3-5，避免对Redis造成过大压力
    --scan-count scan命令每次扫描的key数量
    --delete-interval 每次删除操作之间的间隔时间（毫秒），默认为100ms。设置为0表示不添加间隔，但要注意这可能会对Redis造成较大压力
    --connect-timeout Redis连接超时时间（秒），默认5秒
    --max-retries 操作失败时的最大重试次数，默认3次
    --scan-threads 每个Redis节点的scan线程数，默认1
    --delete-threads 每个Redis节点的delete线程数，默认2
    --single-node 是否使用单节点模式，默认False。如果为True，则只使用第一个Redis节点，并使用pipeline批量删除

    示例命令：
    # 先进行空跑测试
    python3 ./delete_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --max-threads 3 --scan-count 1000 --delete-interval 100 --dry-run True > audit.log 
    
    # 确认无误后执行实际删除（无间隔）
    python3 ./delete_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --max-threads 3 --scan-count 1000 --delete-interval 0 --dry-run False > audit.log 

    # 单节点模式（使用pipeline）
    python3 ./delete_prefix_keys.py --redis-ips 10.74.110.58 --prefix "key:" --single-node True --dry-run False > audit.log
"""

class RedisKeyDeleter:
    def __init__(self, redis_ips: List[str], prefix: str, max_threads: int = 10, scan_count: int = 1000,
                 dry_run: bool = True, delete_interval: float = 0.1, connect_timeout: int = 5, max_retries: int = 3,
                 scan_threads: int = 1, delete_threads: int = 2, single_node: bool = False):
        self.dry_run = dry_run
        self.redis_ips = redis_ips
        self.prefix = prefix
        self.max_threads = max_threads
        self.scan_count = scan_count
        self.delete_interval = delete_interval
        self.connect_timeout = connect_timeout
        self.max_retries = max_retries
        self.scan_threads = scan_threads
        self.delete_threads = delete_threads
        self.single_node = single_node
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads)
        self.stop_event = threading.Event()
        self.key_queue = Queue()
        self.scan_lock = Lock()
        self.delete_lock = Lock()
        self.scan_semaphore = threading.Semaphore(scan_threads)
        self.delete_semaphore = threading.Semaphore(delete_threads)
        
        # 为每个Redis节点创建连接池
        self.connection_pools = {
            ip: ConnectionPool(
                host=ip,
                port=6379,
                db=0,
                max_connections=max_threads,
                socket_timeout=connect_timeout,
                socket_connect_timeout=connect_timeout
            )
            for ip in redis_ips
        }

    def _retry_operation(self, operation, *args, **kwargs):
        """重试机制"""
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except RedisError as e:
                if attempt == self.max_retries - 1:
                    print(f"Operation failed after {self.max_retries} attempts: {str(e)}", file=sys.stderr)
                    raise
                time.sleep(1)  # 重试前等待1秒

    def _scan_keys(self, redis_client: redis.StrictRedis):
        """扫描key并放入队列"""
        cursor = 0
        while not self.stop_event.is_set():
            try:
                with self.scan_semaphore:
                    cursor, keys = self._retry_operation(
                        redis_client.scan,
                        cursor,
                        match=self.prefix + '*',
                        count=self.scan_count
                    )
                    
                    if keys:
                        for key in keys:
                            self.key_queue.put(key)
                    
                    if cursor == 0:
                        break
            except Exception as e:
                print(f"Error during scan operation: {str(e)}", file=sys.stderr)
                break

    def _delete_keys(self, redis_client: redis.StrictRedis):
        """从队列中获取key并删除"""
        pipeline = redis_client.pipeline(transaction=False)
        pipeline_size = 0
        max_pipeline_size = 200  # 每200个命令执行一次pipeline

        while not self.stop_event.is_set():
            try:
                key = self.key_queue.get(timeout=1)  # 1秒超时
                if self.dry_run:
                    print(f"dry run deleted key: {key}")
                else:
                    with self.delete_semaphore:
                        try:
                            print(f"{key.decode('utf-8')}")
                            pipeline.delete(key)
                            pipeline_size += 1
                            
                            if pipeline_size >= max_pipeline_size:
                                pipeline.execute()
                                pipeline_size = 0
                                if self.delete_interval > 0:
                                    time.sleep(self.delete_interval)
                        except Exception as e:
                            print(f"Error deleting key {key}: {str(e)}", file=sys.stderr)
            except Queue.Empty:
                if pipeline_size > 0:
                    pipeline.execute()
                    pipeline_size = 0
                continue
            except Exception as e:
                print(f"Error in delete thread: {str(e)}", file=sys.stderr)

    def _process_redis_node(self, ip: str):
        """处理单个Redis节点"""
        try:
            r = redis.StrictRedis(connection_pool=self.connection_pools[ip])
            
            # 创建scan线程
            scan_threads = []
            for _ in range(self.scan_threads):
                t = threading.Thread(target=self._scan_keys, args=(r,))
                t.daemon = True
                t.start()
                scan_threads.append(t)
            
            # 创建delete线程
            delete_threads = []
            for _ in range(self.delete_threads):
                t = threading.Thread(target=self._delete_keys, args=(r,))
                t.daemon = True
                t.start()
                delete_threads.append(t)
            
            # 等待所有线程完成
            for t in scan_threads:
                t.join()
            
            # 等待队列清空
            while not self.key_queue.empty() and not self.stop_event.is_set():
                time.sleep(0.1)
            
            # 等待delete线程完成
            for t in delete_threads:
                t.join()
                
        except Exception as e:
            print(f"Error processing Redis node {ip}: {str(e)}", file=sys.stderr)
        finally:
            if 'r' in locals():
                r.close()

    def _process_single_node(self):
        """单节点模式处理"""
        try:
            ip = self.redis_ips[0]
            r = redis.StrictRedis(connection_pool=self.connection_pools[ip])
            pipeline = r.pipeline(transaction=False)
            pipeline_size = 0
            max_pipeline_size = 200

            cursor = 0
            while not self.stop_event.is_set():
                try:
                    cursor, keys = self._retry_operation(
                        r.scan,
                        cursor,
                        match=self.prefix + '*',
                        count=self.scan_count
                    )
                    
                    if keys:
                        for key in keys:
                            if self.dry_run:
                                print(f"dry run deleted key: {key}")
                            else:
                                print(f"{key.decode('utf-8')}")
                                pipeline.delete(key)
                                pipeline_size += 1
                                
                                if pipeline_size >= max_pipeline_size:
                                    pipeline.execute()
                                    pipeline_size = 0
                                    if self.delete_interval > 0:
                                        time.sleep(self.delete_interval)
                    
                    if cursor == 0:
                        break
                except Exception as e:
                    print(f"Error during scan operation: {str(e)}", file=sys.stderr)
                    break

            # 执行剩余的pipeline命令
            if pipeline_size > 0:
                pipeline.execute()
                
        except Exception as e:
            print(f"Error in single node mode: {str(e)}", file=sys.stderr)
        finally:
            if 'r' in locals():
                r.close()

    def delete_keys(self):
        try:
            if self.single_node:
                self._process_single_node()
            else:
                with self.executor:
                    futures = [self.executor.submit(self._process_redis_node, ip) for ip in self.redis_ips]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Error in thread: {str(e)}", file=sys.stderr)
        finally:
            # 清理连接池
            for pool in self.connection_pools.values():
                pool.disconnect()
        
        print("Finished deleting keys from all Redis nodes.", file=sys.stderr)

    def signal_handler(self, sig, frame):
        print("Ctrl+C pressed. Shutting down gracefully.", file=sys.stderr)
        self.stop_event.set()
        self.executor.shutdown(wait=False)
        # 清理连接池
        for pool in self.connection_pools.values():
            pool.disconnect()
        print("Exiting...", file=sys.stderr)
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete Redis keys with a specified prefix.")
    parser.add_argument('--dry-run', type=str, default=True, required=True, help='prints keys but do not delete keys')
    parser.add_argument('--redis-ips', type=str, required=True, help='Comma-separated list of Redis IPs，主节点ip')
    parser.add_argument('--prefix', type=str, required=True, help='Prefix of the Redis keys to delete')
    parser.add_argument('--max-threads', type=int, default=10, help='Maximum number of threads to use (default is 10)')
    parser.add_argument('--scan-count', type=int, default=1000, help='Maximum number of scan to use (default is 1000)')
    parser.add_argument('--delete-interval', type=float, default=100, help='delete interval in milliseconds, 0 means no interval')
    parser.add_argument('--connect-timeout', type=int, default=5, help='Redis connection timeout in seconds (default is 5)')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retries for failed operations (default is 3)')
    parser.add_argument('--scan-threads', type=int, default=1, help='Number of scan threads per Redis node (default is 1)')
    parser.add_argument('--delete-threads', type=int, default=2, help='Number of delete threads per Redis node (default is 2)')
    parser.add_argument('--single-node', type=bool, default=False, help='Use single node mode with pipeline (default is False)')

    args = parser.parse_args()

    redis_ips = args.redis_ips.split(',')
    dry_run = True
    if args.dry_run == 'False' or args.dry_run == 'false':
        dry_run = False
    prefix = args.prefix
    max_threads = args.max_threads
    scan_count = args.scan_count
    delete_interval = args.delete_interval / 1000  # Convert milliseconds to seconds
    connect_timeout = args.connect_timeout
    max_retries = args.max_retries
    scan_threads = args.scan_threads
    delete_threads = args.delete_threads
    single_node = args.single_node

    if prefix == "" or prefix is None:
        print("prefix cannot be empty", file=sys.stderr)
        exit(1)
    elif '*' in prefix:
        print("prefix cannot contain '*'", file=sys.stderr)
        exit(1)

    print(
        f'redis ips: {redis_ips}, prefix: {prefix}, max_threads: {max_threads}, scan_count: {scan_count}, '
        f'dry_run: {dry_run}, delete_interval: {delete_interval}, connect_timeout: {connect_timeout}, '
        f'max_retries: {max_retries}, scan_threads: {scan_threads}, delete_threads: {delete_threads}, '
        f'single_node: {single_node}', file=sys.stderr)
    
    deleter = RedisKeyDeleter(
        redis_ips=redis_ips,
        prefix=prefix,
        max_threads=max_threads,
        scan_count=scan_count,
        dry_run=dry_run,
        delete_interval=delete_interval,
        connect_timeout=connect_timeout,
        max_retries=max_retries,
        scan_threads=scan_threads,
        delete_threads=delete_threads,
        single_node=single_node
    )
    signal.signal(signal.SIGINT, deleter.signal_handler)
    deleter.delete_keys()
