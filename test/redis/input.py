# encoding: utf-8
import redis

# 批量刷入redis数据

if __name__ == '__main__':
    source_host = "127.0.0.1"
    port = 6379
    conn = redis.Redis(host=source_host, port=port, db=0)
    with open('/Users/jiayun/Downloads/tmp/brand_user_id.sql', 'r') as file:
        lines = file.readlines()
        print(len(lines))
        count = 0
        pipeline = conn.pipeline()
        batch = 1
        for line in lines:
            count = count + 1
            line = line.replace("\n", "")
            pipeline.sadd("k1", line)
            if count > 10000:
                print(f'batch={batch}')
                pipeline.execute()
                count = 0
                batch = batch + 1
                pipeline = conn.pipeline()
        pipeline.execute()
