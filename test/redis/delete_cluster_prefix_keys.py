#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import signal
import sys
import time
import redis
import argparse
import concurrent.futures
from redis.exceptions import RedisError, ResponseError, ReadOnlyError
from threading import Lock

"""
    用于并发删除多个Redis节点的前缀key
    支持并发处理多个master节点，每个节点使用scan扫描出前缀key，然后使用pipeline批量删除
    
    参数说明：
    --redis-ips Redis节点IP列表，用逗号分隔
    --prefix 要删除的key前缀
    --scan-count scan命令每次扫描的key数量，默认1000
    --pipeline-size pipeline批量删除的大小，默认200
    --delete-interval 每次pipeline执行后的间隔时间（毫秒），默认100ms
    --dry-run 是否只打印不删除，默认True
    --connect-timeout Redis连接超时时间（秒），默认5秒
    --max-retries 操作失败时的最大重试次数，默认3次
    --max-workers 最大并发处理节点数，默认3
    --port Redis端口，默认6379
    --password Redis密码，默认无
    --only-master 是否只连接master节点，默认True
    --skip-slave 是否跳过slave节点，默认True
    --output-file 输出文件路径，默认audit.log

    示例命令：
    # 先进行空跑测试
    python3 ./delete_cluster_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --dry-run True --output-file audit.log
    
    # 确认无误后执行实际删除
    python3 ./delete_cluster_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --dry-run False --output-file audit.log
"""

class ClusterKeyDeleter:
    def __init__(self, redis_ips, prefix, scan_count=1000, pipeline_size=200, delete_interval=100,
                 dry_run=True, connect_timeout=5, max_retries=3, max_workers=3, port=6379, password=None,
                 only_master=True, skip_slave=True, output_file='audit.log'):
        self.redis_ips = redis_ips
        self.prefix = prefix
        self.scan_count = scan_count
        self.pipeline_size = pipeline_size
        self.delete_interval = delete_interval / 1000  # 转换为秒
        self.dry_run = dry_run
        self.connect_timeout = connect_timeout
        self.max_retries = max_retries
        self.max_workers = max_workers
        self.port = port
        self.password = password
        self.only_master = only_master
        self.skip_slave = skip_slave
        self.output_file = output_file
        self.stop_event = False
        self.total_deleted = 0
        self.total_deleted_lock = Lock()
        self.print_lock = Lock()
        self.file_lock = Lock()

    def _retry_operation(self, operation, *args, **kwargs):
        """重试机制"""
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except RedisError as e:
                if attempt == self.max_retries - 1:
                    self._safe_print(f"Operation failed after {self.max_retries} attempts: {str(e)}", True)
                    raise
                time.sleep(1)  # 重试前等待1秒

    def _safe_print(self, message, is_error=False):
        """线程安全的打印到控制台"""
        with self.print_lock:
            if is_error:
                print(message, file=sys.stderr)
            else:
                print(message)

    def _write_to_file(self, message):
        """线程安全的写入文件"""
        with self.file_lock:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                f.write(message + '\n')

    def _is_master(self, redis_client):
        """检查节点是否为master"""
        try:
            info = redis_client.info('replication')
            role = info.get('role')
            if role == 'master':
                return True
            elif role == 'slave':
                master_host = info.get('master_host', 'unknown')
                master_port = info.get('master_port', 'unknown')
                self._safe_print(f"\n{'='*50}\n警告：节点是slave节点！\n主节点信息：{master_host}:{master_port}\n{'='*50}\n", True)
                return False
            else:
                self._safe_print(f"\n{'='*50}\n警告：未知的节点角色：{role}\n{'='*50}\n", True)
                return False
        except Exception as e:
            self._safe_print(f"\n{'='*50}\n错误：检查节点角色失败：{str(e)}\n{'='*50}\n", True)
            return False

    def _process_node(self, ip):
        """处理单个Redis节点"""
        try:
            # 创建Redis连接
            r = redis.StrictRedis(
                host=ip,
                port=self.port,
                password=self.password,
                socket_timeout=self.connect_timeout,
                socket_connect_timeout=self.connect_timeout
            )
            
            # 检查节点角色
            is_master = self._is_master(r)
            if not is_master:
                if self.only_master:
                    if self.skip_slave:
                        self._safe_print(f"\n{'='*50}\n跳过slave节点：{ip}\n{'='*50}\n", True)
                        return
                    else:
                        self._safe_print(f"\n{'='*50}\n警告：{ip} 是slave节点，将尝试处理但可能会失败\n{'='*50}\n", True)
                else:
                    self._safe_print(f"\n{'='*50}\n处理slave节点：{ip}\n{'='*50}\n", True)
            
            # 创建pipeline
            pipeline = r.pipeline(transaction=False)
            pipeline_size = 0
            node_deleted = 0
            
            # 开始scan
            cursor = 0
            while not self.stop_event:
                try:
                    # 扫描key
                    cursor, keys = self._retry_operation(
                        r.scan,
                        cursor,
                        match=self.prefix + '*',
                        count=self.scan_count
                    )
                    
                    if keys:
                        for key in keys:
                            if self.dry_run:
                                self._write_to_file(f"dry run deleted key: {key}")
                            else:
                                try:
                                    self._write_to_file(f"{key.decode('utf-8')}")
                                    pipeline.delete(key)
                                    pipeline_size += 1
                                    node_deleted += 1
                                    
                                    # 当pipeline达到指定大小时执行
                                    if pipeline_size >= self.pipeline_size:
                                        try:
                                            pipeline.execute()
                                            with self.total_deleted_lock:
                                                self.total_deleted += pipeline_size
                                            self._safe_print(f"Deleted {pipeline_size} keys from {ip}, node total: {node_deleted}, global total: {self.total_deleted}", True)
                                            pipeline_size = 0
                                            if self.delete_interval > 0:
                                                time.sleep(self.delete_interval)
                                        except ReadOnlyError:
                                            if not is_master:
                                                self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 是slave节点，跳过删除操作\n{'='*50}\n", True)
                                            else:
                                                self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 变为只读状态，跳过删除操作\n{'='*50}\n", True)
                                            return
                                        except ResponseError as e:
                                            self._safe_print(f"Pipeline execution error on {ip}: {str(e)}", True)
                                except ReadOnlyError:
                                    if not is_master:
                                        self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 是slave节点，跳过删除操作\n{'='*50}\n", True)
                                    else:
                                        self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 变为只读状态，跳过删除操作\n{'='*50}\n", True)
                                    return
                    
                    if cursor == 0:
                        break
                        
                except Exception as e:
                    self._safe_print(f"Error during scan operation on {ip}: {str(e)}", True)
                    break
            
            # 执行剩余的pipeline命令
            if pipeline_size > 0 and not self.dry_run:
                try:
                    pipeline.execute()
                    with self.total_deleted_lock:
                        self.total_deleted += pipeline_size
                    self._safe_print(f"Deleted {pipeline_size} keys from {ip}, node total: {node_deleted}, global total: {self.total_deleted}", True)
                except ReadOnlyError:
                    if not is_master:
                        self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 是slave节点，跳过删除操作\n{'='*50}\n", True)
                    else:
                        self._safe_print(f"\n{'='*50}\n错误：节点 {ip} 变为只读状态，跳过删除操作\n{'='*50}\n", True)
                except ResponseError as e:
                    self._safe_print(f"Pipeline execution error on {ip}: {str(e)}", True)
            
            self._safe_print(f"Finished processing node {ip}, total deleted: {node_deleted}", True)
            
        except Exception as e:
            self._safe_print(f"Error processing node {ip}: {str(e)}", True)
        finally:
            if 'r' in locals():
                r.close()

    def delete_keys(self):
        """并发处理所有Redis节点"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._process_node, ip) for ip in self.redis_ips]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self._safe_print(f"Error in thread: {str(e)}", True)
        
        self._safe_print(f"Finished deleting keys from all Redis nodes. Total deleted: {self.total_deleted}", True)

    def signal_handler(self, sig, frame):
        """处理Ctrl+C信号"""
        self._safe_print("Ctrl+C pressed. Shutting down gracefully.", True)
        self.stop_event = True
        self._safe_print("Exiting...", True)
        sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Delete Redis keys with a specified prefix from multiple nodes concurrently.")
    parser.add_argument('--redis-ips', type=str, required=True, help='Comma-separated list of Redis node IPs')
    parser.add_argument('--prefix', type=str, required=True, help='Prefix of the Redis keys to delete')
    parser.add_argument('--scan-count', type=int, default=1000, help='Number of keys to scan per iteration (default: 1000)')
    parser.add_argument('--pipeline-size', type=int, default=200, help='Number of keys to delete in one pipeline (default: 200)')
    parser.add_argument('--delete-interval', type=float, default=100, help='Interval between pipeline executions in milliseconds (default: 100)')
    parser.add_argument('--dry-run', type=str, default='True', help='Only print keys without deleting (default: True)')
    parser.add_argument('--connect-timeout', type=int, default=5, help='Redis connection timeout in seconds (default: 5)')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retries for failed operations (default: 3)')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum number of concurrent nodes to process (default: 3)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--password', type=str, help='Redis password (default: None)')
    parser.add_argument('--only-master', type=str, default='True', help='Only connect to master nodes (default: True)')
    parser.add_argument('--skip-slave', type=str, default='True', help='Skip slave nodes (default: True)')
    parser.add_argument('--output-file', type=str, default='audit.log', help='Output file path (default: audit.log)')

    args = parser.parse_args()

    # 处理参数
    redis_ips = args.redis_ips.split(',')
    dry_run = True
    if args.dry_run.lower() == 'false':
        dry_run = False
    
    only_master = True
    if args.only_master.lower() == 'false':
        only_master = False
    
    skip_slave = True
    if args.skip_slave.lower() == 'false':
        skip_slave = False

    if not args.prefix:
        print("prefix cannot be empty", file=sys.stderr)
        sys.exit(1)
    elif '*' in args.prefix:
        print("prefix cannot contain '*'", file=sys.stderr)
        sys.exit(1)

    # 创建删除器实例
    deleter = ClusterKeyDeleter(
        redis_ips=redis_ips,
        prefix=args.prefix,
        scan_count=args.scan_count,
        pipeline_size=args.pipeline_size,
        delete_interval=args.delete_interval,
        dry_run=dry_run,
        connect_timeout=args.connect_timeout,
        max_retries=args.max_retries,
        max_workers=args.max_workers,
        port=args.port,
        password=args.password,
        only_master=only_master,
        skip_slave=skip_slave,
        output_file=args.output_file
    )

    # 设置信号处理
    signal.signal(signal.SIGINT, deleter.signal_handler)

    # 打印配置信息
    print(
        f'Configuration:\n'
        f'  Redis IPs: {redis_ips}\n'
        f'  Prefix: {args.prefix}\n'
        f'  Scan count: {args.scan_count}\n'
        f'  Pipeline size: {args.pipeline_size}\n'
        f'  Delete interval: {args.delete_interval}ms\n'
        f'  Dry run: {dry_run}\n'
        f'  Connect timeout: {args.connect_timeout}s\n'
        f'  Max retries: {args.max_retries}\n'
        f'  Max workers: {args.max_workers}\n'
        f'  Port: {args.port}\n'
        f'  Password: {"*" * 8 if args.password else "None"}\n'
        f'  Only master: {only_master}\n'
        f'  Skip slave: {skip_slave}\n'
        f'  Output file: {args.output_file}',
        file=sys.stderr
    )

    # 开始删除
    deleter.delete_keys()

if __name__ == "__main__":
    main() 