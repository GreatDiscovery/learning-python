import time
import concurrent.futures

import redis

key = "k1"
field = "f1"


# sh
def task1(index):
    print(f"Task {index} started")
    # 模拟任务执行
    # 连接到本地 Redis 服务器
    redis_conn = redis.StrictRedis(host='10.13.246.139', port=12345, decode_responses=True)

    while True:
        # 使用 INCR 操作，如果 key 不存在则创建并将值设为 1
        # redis_conn.incr(key, 1)
        redis_conn.hincrby(key, field)
    print(f"Task {index} completed")


# hz
def task2(index):
    print(f"Task {index} started")
    # 模拟任务执行
    # 连接到本地 Redis 服务器
    redis_conn = redis.StrictRedis(host='10.214.236.185', port=12345, decode_responses=True)
    while True:
        # 使用 INCR 操作，如果 key 不存在则创建并将值设为 1
        # redis_conn.incr(key, 1)
        redis_conn.hincrby(key, field)

    print(f"Task {index} completed")


# nj
def task3(index):
    print(f"Task {index} started")
    # 模拟任务执行
    # 连接到本地 Redis 服务器
    redis_conn = redis.StrictRedis(host='10.212.190.235', port=12345, decode_responses=True)

    key = "k1"
    while True:
        # 使用 INCR 操作，如果 key 不存在则创建并将值设为 1
        # redis_conn.incr(key, 1)
        redis_conn.hincrby(key, field)
    print(f"Task {index} completed")


def main():
    # 创建线程池，设置最大线程数为3
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # 提交任务到线程池
        futures = [executor.submit(task1, 1), executor.submit(task2, 2), executor.submit(task3, 3)]

        # 等待所有任务完成
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
