#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
设计要点：
1. 核心功能：并发删除多Redis节点前缀key，使用scan+pipeline提高效率。同时对多个Redis节点进行并发操作
2. 安全性：自动识别slave节点，支持dry-run预演，提前查看删除的key数量；同时记录audit日志，记录所有删除的key，方便日后审计
3. 健壮性：完善的错误处理和重试机制，支持优雅退出，避免因单个节点问题导致整个脚本崩溃
4. 性能优化：使用Redis连接池管理，批量删除，文件缓冲写入，避免频繁的文件操作
5. 监控统计：实时统计信息，详细日志记录，内存监控。实时打印统计信息，方便运维查看
6. 配置灵活：丰富的命令行参数，可调整的并发度和超时时间
7. 资源管理：连接池、内存限制、任务超时控制

使用前pip3 install redis
psutil模块可选，用于显示内存使用情况
"""

import signal
import sys
import time
import redis
import argparse
import concurrent.futures
from redis.exceptions import RedisError, ResponseError, ReadOnlyError
from threading import Lock
from queue import Queue
import threading
import logging
import os
import json
from datetime import datetime
import gzip
from typing import List, Dict, Any

# 尝试导入psutil，如果失败则设置为None
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    psutil = None

"""
    用于并发删除多个Redis节点的前缀key
    支持并发处理多个master节点，每个节点使用scan扫描出前缀key，然后使用pipeline批量删除。
    ⚠️：使用前先对单个master节点进行dry-run测试，确保脚本没有问题后，再对单个master进行实际删除。最后再对多个master进行批量删除
    

    使用前pip3 install redis psutil
    参数说明：
    --redis-ips Redis节点IP列表，用逗号分隔，可以输入全部的节点，程序会自动跳过slave节点
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
    --buffer-size 文件写入缓冲区大小，默认1000
    --compress 是否压缩输出文件，默认False
    --log-level 日志级别，默认INFO
    --max-memory 最大内存使用限制（MB），默认1024
    --stats-interval 统计信息输出间隔（秒），默认60
    --task-timeout 单个任务超时时间（秒），默认3600秒
    --overall-timeout 整体超时时间（秒），默认86400秒

    示例命令：
    # 先进行空跑测试
    python3 ./redis_delete_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --dry-run True --output-file audit.log
    
    # 确认无误后执行实际删除
    python3 ./redis_delete_prefix_keys.py --redis-ips 10.74.110.58,10.74.40.101,10.74.204.2 --prefix "key:" --dry-run False --output-file audit.log
"""

class FileWriter:
    def __init__(self, output_file: str, buffer_size: int = 1000, compress: bool = False):
        self.output_file = output_file
        self.buffer_size = buffer_size
        self.compress = compress
        self.buffer: List[str] = []
        self.buffer_lock = Lock()
        self.total_written = 0
        self.last_flush_time = time.time()
        self.flush_interval = 60  # 60秒强制刷新一次
        logging.info("File writer initialized")

    def _write_lines(self, lines: List[str]):
        """写入行到文件"""
        try:
            mode = 'ab' if self.compress else 'a'
            open_func = gzip.open if self.compress else open
            with open_func(self.output_file, mode, encoding='utf-8' if not self.compress else None) as f:
                content = '\n'.join(lines) + '\n'
                if self.compress:
                    f.write(content.encode('utf-8'))
                else:
                    f.write(content)
            self.total_written += len(lines)
        except Exception as e:
            logging.error(f"Error writing to file: {str(e)}")

    def write(self, message: str):
        """添加消息到缓冲区，如果缓冲区满了就写入文件"""
        with self.buffer_lock:
            self.buffer.append(message)
            current_time = time.time()
            should_flush = (len(self.buffer) >= self.buffer_size or 
                          current_time - self.last_flush_time >= self.flush_interval)
            
            if should_flush:
                lines = self.buffer
                self.buffer = []
                self.last_flush_time = current_time
                self._write_lines(lines)

    def stop(self):
        """写入剩余的数据并关闭文件"""
        with self.buffer_lock:
            if self.buffer:
                logging.info(f"Writing remaining {len(self.buffer)} lines...")
                self._write_lines(self.buffer)
                self.buffer = []
        logging.info(f"File writer stopped, total written: {self.total_written} lines")

class ClusterKeyDeleter:
    def __init__(self, redis_ips: List[str], prefix: str, scan_count: int = 1000, pipeline_size: int = 200,
                 delete_interval: float = 100, dry_run: bool = True, connect_timeout: int = 5, max_retries: int = 3,
                 max_workers: int = 3, port: int = 6379, password: str = None, only_master: bool = True,
                 skip_slave: bool = True, output_file: str = 'audit.log', buffer_size: int = 1000,
                 compress: bool = False, log_level: str = 'INFO', max_memory: int = 1024,
                 stats_interval: int = 60, task_timeout: int = 3600, overall_timeout: int = 86400):
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
        self.compress = compress
        self.max_memory = max_memory * 1024 * 1024  # 转换为字节
        self.stats_interval = stats_interval
        self.task_timeout = task_timeout  # 单个任务超时时间（秒）
        self.overall_timeout = overall_timeout  # 整体超时时间（秒）
        
        # 设置日志
        self._setup_logging(log_level)
        
        # 创建连接池
        self.pools = {}  # 为每个节点创建独立的连接池
        for ip in redis_ips:
            self.pools[ip] = redis.ConnectionPool(
                host=ip,
                port=port,
                password=password,
                max_connections=2,  # 每个节点最多2个连接
                socket_timeout=connect_timeout,
                socket_connect_timeout=connect_timeout,
                decode_responses=True  # 自动解码响应
            )
        
        self.file_writer = FileWriter(output_file, buffer_size, compress)
        self.stop_event = False
        self.total_deleted = 0
        self.total_deleted_lock = Lock()
        self.print_lock = Lock()
        self.stats = {
            'start_time': time.time(),
            'nodes_processed': 0,
            'nodes_skipped': 0,
            'errors': 0,
            'retries': 0
        }
        self.stats_lock = Lock()
        self.processed_nodes = set()  # 用于跟踪已处理的节点
        
        # 启动统计信息线程
        self.stats_thread = threading.Thread(target=self._stats_worker, daemon=True)
        self.stats_thread.start()

    def _setup_logging(self, log_level: str):
        """设置日志配置"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        
        # 创建日志记录器
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # 创建文件处理器
        file_handler = logging.FileHandler('redis_key_deleter.log')
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(console_handler)

    def _stats_worker(self):
        """统计信息输出线程"""
        while not self.stop_event:
            self._print_stats()
            time.sleep(self.stats_interval)

    def _print_stats(self):
        """打印统计信息"""
        with self.stats_lock:
            current_time = time.time()
            duration = current_time - self.stats['start_time']
            
            stats_msg = (
                f"\n{'='*50}\n"
                f"运行时间: {duration:.2f}秒\n"
                f"已处理节点: {self.stats['nodes_processed']}\n"
                f"跳过节点: {self.stats['nodes_skipped']}\n"
                f"删除key数: {self.total_deleted}\n"
                f"错误数: {self.stats['errors']}\n"
                f"重试次数: {self.stats['retries']}\n"
            )
            
            if HAS_PSUTIL:
                memory_usage = psutil.Process().memory_info().rss
                stats_msg += f"内存使用: {memory_usage/1024/1024:.2f}MB\n"
            
            stats_msg += (
                f"文件写入: {self.file_writer.total_written}行\n"
                f"{'='*50}\n"
            )
            self._safe_print(stats_msg)

    def _check_memory(self):
        """检查内存使用"""
        if not HAS_PSUTIL:
            return True
        memory_usage = psutil.Process().memory_info().rss
        if memory_usage > self.max_memory:
            logging.warning(f"Memory usage ({memory_usage/1024/1024:.2f}MB) exceeds limit ({self.max_memory/1024/1024:.2f}MB)")
            return False
        return True

    def _retry_operation(self, operation, *args, **kwargs):
        """重试机制"""
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except RedisError as e:
                with self.stats_lock:
                    self.stats['retries'] += 1
                if attempt == self.max_retries - 1:
                    logging.error(f"Operation failed after {self.max_retries} attempts: {str(e)}")
                    with self.stats_lock:
                        self.stats['errors'] += 1
                    raise
                time.sleep(1)  # 重试前等待1秒

    def _safe_print(self, message: str, is_error: bool = False):
        """线程安全的打印到控制台"""
        with self.print_lock:
            if is_error:
                print(message, file=sys.stderr)
            else:
                print(message)

    def _write_to_file(self, message: str):
        """写入消息到文件缓冲区"""
        self.file_writer.write(message)

    def _is_master(self, redis_client) -> bool:
        """检查节点是否为master"""
        try:
            info = redis_client.info('replication')
            role = info.get('role')
            current_ip = redis_client.connection_pool.connection_kwargs.get('host')
            if role == 'master':
                return True
            elif role == 'slave':
                master_host = info.get('master_host', 'unknown')
                master_port = info.get('master_port', 'unknown')
                warning_msg = (
                    f"\n{'='*80}\n"
                    f"⚠️ 警告：当前节点是slave节点！\n"
                    f"当前节点：{current_ip}\n"
                    f"主节点信息：{master_host}:{master_port}\n"
                    f"{'='*80}\n"
                )
                logging.warning(warning_msg)
                return False
            else:
                warning_msg = (
                    f"\n{'='*80}\n"
                    f"⚠️ 警告：未知的节点角色：{role}\n"
                    f"当前节点：{current_ip}\n"
                    f"{'='*80}\n"
                )
                logging.warning(warning_msg)
                return False
        except Exception as e:
            error_msg = (
                f"\n{'='*80}\n"
                f"❌ 错误：检查节点角色失败\n"
                f"节点：{redis_client.connection_pool.connection_kwargs.get('host')}\n"
                f"错误信息：{str(e)}\n"
                f"{'='*80}\n"
            )
            logging.error(error_msg)
            return False

    def _print_unprocessed_nodes(self):
        """打印未处理的节点信息"""
        processed = self.processed_nodes
        unprocessed = set(self.redis_ips) - processed
        
        if unprocessed:
            warning_msg = (
                f"\n{'='*80}\n"
                f"⚠️ 警告：以下节点未被处理：\n"
                f"{','.join(unprocessed)}\n"
                f"{'='*80}\n"
            )
            self._safe_print(warning_msg)  # 只使用控制台打印，不使用logging

    def _process_node(self, ip: str):
        """处理单个Redis节点"""
        r = None
        try:
            # 创建Redis连接
            r = redis.StrictRedis(
                connection_pool=self.pools[ip]
            )
            
            # 检查节点角色
            is_master = self._is_master(r)
            if not is_master:
                if self.only_master:
                    if self.skip_slave:
                        logging.info(f"Skipping slave node: {ip}")
                        with self.stats_lock:
                            self.stats['nodes_skipped'] += 1
                        self.processed_nodes.add(ip)  # 标记为已处理
                        return
                    else:
                        logging.warning(f"Node {ip} is slave, will try to process but may fail")
                else:
                    logging.info(f"Processing slave node: {ip}")
            
            # 创建pipeline
            pipeline = r.pipeline(transaction=False)
            pipeline_size = 0
            node_deleted = 0
            
            # 开始scan
            cursor = 0
            while not self.stop_event:
                if not self._check_memory():
                    logging.error("Memory limit exceeded, stopping node processing")
                    break
                    
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
                            if self.stop_event:  # 检查是否需要停止
                                return
                            if self.dry_run:
                                self._write_to_file(f"dry run deleted key: {key}")
                            else:
                                try:
                                    self._write_to_file(f"{key}")
                                    pipeline.delete(key)
                                    pipeline_size += 1
                                    node_deleted += 1
                                    
                                    # 当pipeline达到指定大小时执行
                                    if pipeline_size >= self.pipeline_size:
                                        try:
                                            pipeline.execute()
                                            with self.total_deleted_lock:
                                                self.total_deleted += pipeline_size
                                            logging.info(f"Deleted {pipeline_size} keys from {ip}, node total: {node_deleted}, global total: {self.total_deleted}")
                                            pipeline_size = 0
                                            if self.delete_interval > 0:
                                                time.sleep(self.delete_interval)
                                        except ReadOnlyError:
                                            if not is_master:
                                                logging.error(f"Node {ip} is slave, skipping delete operation")
                                            else:
                                                logging.error(f"Node {ip} became read-only, skipping delete operation")
                                            return
                                        except ResponseError as e:
                                            logging.error(f"Pipeline execution error on {ip}: {str(e)}")
                                except ReadOnlyError:
                                    if not is_master:
                                        logging.error(f"Node {ip} is slave, skipping delete operation")
                                    else:
                                        logging.error(f"Node {ip} became read-only, skipping delete operation")
                                    return
                    
                    if cursor == 0:
                        break
                        
                except Exception as e:
                    logging.error(f"Error during scan operation on {ip}: {str(e)}")
                    break
            
            # 执行剩余的pipeline命令
            if pipeline_size > 0 and not self.dry_run and not self.stop_event:
                try:
                    pipeline.execute()
                    with self.total_deleted_lock:
                        self.total_deleted += pipeline_size
                    logging.info(f"Deleted {pipeline_size} keys from {ip}, node total: {node_deleted}, global total: {self.total_deleted}")
                except ReadOnlyError:
                    if not is_master:
                        logging.error(f"Node {ip} is slave, skipping delete operation")
                    else:
                        logging.error(f"Node {ip} became read-only, skipping delete operation")
                except ResponseError as e:
                    logging.error(f"Pipeline execution error on {ip}: {str(e)}")
            
            with self.stats_lock:
                self.stats['nodes_processed'] += 1
            logging.info(f"Finished processing node {ip}, total deleted: {node_deleted}")
            self.processed_nodes.add(ip)  # 标记为已处理
            
        except Exception as e:
            logging.error(f"Error processing node {ip}: {str(e)}")
            with self.stats_lock:
                self.stats['errors'] += 1
        finally:
            if r is not None:
                r.close()

    def delete_keys(self):
        """并发处理所有Redis节点"""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._process_node, ip) for ip in self.redis_ips]
                try:
                    # 等待所有任务完成，设置超时时间
                    for future in concurrent.futures.as_completed(futures, timeout=self.overall_timeout):
                        try:
                            future.result(timeout=self.task_timeout)
                        except concurrent.futures.TimeoutError:
                            logging.error(f"Task execution timeout after {self.task_timeout} seconds")
                        except Exception as e:
                            logging.error(f"Error in thread: {str(e)}")
                except concurrent.futures.TimeoutError:
                    logging.error(f"Overall execution timeout after {self.overall_timeout} seconds")
                finally:
                    # 取消所有未完成的任务
                    for future in futures:
                        if not future.done():
                            future.cancel()
                    logging.info("All tasks completed or cancelled")
        except Exception as e:
            logging.error(f"Error in thread pool: {str(e)}")
        finally:
            # 打印未处理的节点信息
            self._print_unprocessed_nodes()
            
            # 确保所有数据都写入文件
            logging.info("Stopping file writer...")
            self.stop_event = True
            self.file_writer.stop()
            logging.info("File writer stopped")
            
            # 打印最终统计信息
            logging.info("Printing final statistics...")
            self._print_stats()
            
            # 关闭所有连接池
            logging.info("Closing Redis connection pools...")
            for pool in self.pools.values():
                pool.disconnect()
            logging.info("All Redis connection pools closed")
        
        logging.info(f"Finished deleting keys from all Redis nodes. Total deleted: {self.total_deleted}")
        logging.info("Program completed successfully")

    def signal_handler(self, sig, frame):
        """处理Ctrl+C信号"""
        logging.info("Ctrl+C pressed. Shutting down gracefully.")
        self.stop_event = True
        # 关闭所有连接池
        for pool in self.pools.values():
            pool.disconnect()
        self.file_writer.stop()
        self._print_stats()
        logging.info("Exiting...")
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
    parser.add_argument('--output-file', type=str, default='audit.log', help='Output file path for deleted keys (default: audit.log)')
    parser.add_argument('--buffer-size', type=int, default=1000, help='File write buffer size (default: 1000)')
    parser.add_argument('--compress', type=str, default='False', help='Compress output file (default: False)')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (default: INFO)')
    parser.add_argument('--max-memory', type=int, default=1024, help='Maximum memory usage in MB (default: 1024)')
    parser.add_argument('--stats-interval', type=int, default=60, help='Statistics output interval in seconds (default: 60)')
    parser.add_argument('--task-timeout', type=int, default=3600, help='Timeout for each task in seconds (default: 3600)')
    parser.add_argument('--overall-timeout', type=int, default=86400, help='Overall timeout in seconds (default: 86400)')

    args = parser.parse_args()

    # 处理参数
    redis_ips = [ip.strip() for ip in args.redis_ips.split(',') if ip.strip()]
    if not redis_ips:
        print("Error: No valid Redis IPs provided", file=sys.stderr)
        sys.exit(1)
        
    dry_run = True
    if args.dry_run.lower() == 'false':
        dry_run = False
    
    only_master = True
    if args.only_master.lower() == 'false':
        only_master = False
    
    skip_slave = True
    if args.skip_slave.lower() == 'false':
        skip_slave = False
        
    compress = False
    if args.compress.lower() == 'true':
        compress = True

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
        output_file=args.output_file,
        buffer_size=args.buffer_size,
        compress=compress,
        log_level=args.log_level,
        max_memory=args.max_memory,
        stats_interval=args.stats_interval,
        task_timeout=args.task_timeout,
        overall_timeout=args.overall_timeout
    )

    # 设置信号处理
    signal.signal(signal.SIGINT, deleter.signal_handler)

    # 打印配置信息
    config_info = {
        'Redis IPs': redis_ips,
        'Prefix': args.prefix,
        'Scan count': args.scan_count,
        'Pipeline size': args.pipeline_size,
        'Delete interval': f"{args.delete_interval}ms",
        'Dry run': dry_run,
        'Connect timeout': f"{args.connect_timeout}s",
        'Max retries': args.max_retries,
        'Max workers': args.max_workers,
        'Port': args.port,
        'Password': "******" if args.password else "None",
        'Only master': only_master,
        'Skip slave': skip_slave,
        'Output file': args.output_file,
        'Buffer size': args.buffer_size,
        'Compress': compress,
        'Log level': args.log_level,
        'Max memory': f"{args.max_memory}MB",
        'Stats interval': f"{args.stats_interval}s",
        'Task timeout': f"{args.task_timeout}s",
        'Overall timeout': f"{args.overall_timeout}s"
    }
    
    logging.info("Configuration:")
    for key, value in config_info.items():
        logging.info(f"  {key}: {value}")

    # 开始删除
    deleter.delete_keys()

if __name__ == "__main__":
    main() 