# encoding: utf-8
import argparse
import random

import redis

# redis集群写满后，调整key的时间，使其快速过期，快速减少内存
# 可以判断key的过期时间，只有过期时间大于一定时间的才可以更改过期时间，防止过期时间较小的key或者持久化key被更新
# 新的过期时间在[expire_time, 2*expire_time]之间打散，防止同一时间过期问题
# usage: python expire_all_keys.py --host 127.0.0.1 -p 6379 -b 100 -m 'k*' -e 100 -g -2

# Lua script to modify expire time
lua_script = """
local key = KEYS[1]
local new_ttl = tonumber(ARGV[1])
local greater_than = tonumber(ARGV[2])

-- Check if the key has an expiration time (TTL)
local current_ttl = redis.call('TTL', key)

-- if greater_than == -2 then modify every key
if current_ttl == -2 then
    return 0
elseif current_ttl < greater_than then
    -- Key already has an expiration time, do nothing
    return 0  -- Indicate that no changes were made
else
    redis.call('EXPIRE', key, new_ttl)
    return 1  -- Indicate that the expiration time was updated
end
"""


def get_redis_client(hostname: str, port: int):
    r = redis.Redis(host=hostname, port=port, db=0)
    return r


def get_random_num(end_range: int, start_range=0):
    # Generate a random integer within the specified range
    random_number = random.randint(start_range, end_range)
    return random_number


# prevent all keys from expiring at the same time
def need_to_scatter(expire: int):
    return expire > 100


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='expire all keys')
    parser.add_argument('-host', '--hostname', type=str, help='redis hostname', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=str, help='redis port', default='6379')
    parser.add_argument('-e', '--expire_time', type=int, help='redis expire time in seconds', required=True)
    parser.add_argument('-c', '--cluster_mode', type=bool, help='redis cluster mode or not', default=False)
    parser.add_argument('-b', '--batch_size', type=int, help='scan count', default=10000)
    parser.add_argument('-m', '--match', type=str, help='scan match, example: prefix*', default='*')
    parser.add_argument('-g', '--greater_than', type=int, help='only modify key that has expire time and the expire '
                                                               'time must greater than this parameter in seconds, '
                                                               'example: 60',
                        required=True, default=60)
    args = parser.parse_args()
    expire_time = args.expire_time
    if expire_time < 0:
        print(f'expire time must greater than 0, current is {expire_time}')
        exit(1)
    min_time = args.greater_than
    if min_time < 0:
        print('modify all keys')
    else:
        print(f"modify keys whose expire time greater than {min_time}")

    client = get_redis_client(args.hostname, args.port)
    # Calculate SHA1 hash of the Lua script
    script_sha1 = client.script_load(lua_script)

    scatter = need_to_scatter(expire_time)
    for key in client.scan_iter(match=args.match, count=args.batch_size):
        # todo pipeline
        keys_and_args = [key, expire_time, min_time]
        if scatter:
            keys_and_args[1] += get_random_num(expire_time)
        result = client.evalsha(script_sha1, 1, *keys_and_args)
