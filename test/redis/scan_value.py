# encoding: utf-8
import redis
from rediscluster import RedisCluster

source_host = "xxxx"
port = 6379
startup_nodes = [{"host": source_host, "port": port}]
r = redis.Redis(host=source_host, port=6379, db=0)
# r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# 使用 SCAN 命令迭代所有的 key，数据量小还可以用，数据量大了最好离线分析
cursor = 0
while True:
    keys = r.scan(match='v7.SS.available*', cursor=cursor, count=1000)  # 每次迭代 1000 个 key
    cursor = keys[0]
    if cursor == 0:
        break
    for key in keys[1]:
        value = r.get(key)
        if value == "OK":
            print(f'key={key}, value={value}')

