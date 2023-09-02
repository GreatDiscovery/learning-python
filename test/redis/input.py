# encoding: utf-8
import redis

if __name__ == '__main__':
    source_host = "10.0.21.150"
    port = 6379
    conn = redis.Redis(host=source_host, port=port, db=0)
    with open('/Users/jiayun/Downloads/tmp/232.csv', 'r') as file:
        lines = file.readlines()
        print(len(lines))
        count = 0
        pipeline = conn.pipeline()
        batch = 1
        for line in lines:
            count = count + 1
            pipeline.sadd("mykey", line)
            if count > 1000:
                print(f'batch={batch}')
                pipeline.execute()
                count = 0
                batch = batch + 1
                pipeline = conn.pipeline()
