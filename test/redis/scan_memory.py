# encoding: utf-8
from rediscluster import RedisCluster

source_host = "10.0.21.150"
port = 6379
startup_nodes = [{"host": source_host, "port": port}]
r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

key_type = "string:perf_match_item*"  # 需要查询的 key 类型
key_memory_usage = 0  # 初始化 key 内存使用量为 0
total_memory_usage = r.info('memory')['used_memory']  # 获取 Redis 总内存使用量

# 使用 SCAN 命令迭代所有的 key，数据量小还可以用，数据量大了最好离线分析
cursor = 0
while True:
    keys = r.scan(match=key_type, cursor=cursor, count=1000)  # 每次迭代 1000 个 key
    cursor = keys[0]
    if cursor == 0:
        break
    for key in keys[1]:
        if r.type(key) == key_type:  # 如果 key 类型匹配，则统计其内存使用量
            key_memory_usage += r.debug_object(key)['serializedlength']

        # 计算特定类型 key 的内存占比
key_memory_ratio = key_memory_usage / total_memory_usage
print("Memory usage of {} keys: {:.2%}".format(key_type, key_memory_ratio))
