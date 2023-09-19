# encoding: utf-8
import redis

# 给出一份key的文件，拉出所有key的数据并保存到文件里

source_host = "127.0.0.1"
port = 6379
conn = redis.Redis(host=source_host, port=port, db=0)
key_file = "/Users/jiayun/Downloads/tmp/1.csv"
saved_file = "/Users/jiayun/Downloads/tmp/save.csv"


def print_key_value(keys, responses):
    lines = []
    for index in range(len(keys)):
        key = keys[index]
        value = ','.join(item.decode('utf-8') for item in responses[index])
        if value:
            line = f'{key} {value}'
            lines.append(line)
    return lines


if __name__ == '__main__':
    with open(key_file, 'r') as file, open(saved_file, 'w') as file2:
        lines = file.readlines()
        print(f'total_line={len(lines)}')
        count = 0
        pipeline = conn.pipeline()
        batch = 1
        keys = []
        for line in lines:
            count = count + 1
            line = line.replace("\n", "")
            line = line.replace("\"", "")
            keys.append(line)
            pipeline.smembers(line)
            if count > 10000:
                print(f'batch={batch}')
                responses = pipeline.execute()
                lines = print_key_value(keys, responses)
                for l in lines:
                    file2.write(l)
                    file2.write('\n')
                count = 0
                keys = []
                batch = batch + 1
                pipeline = conn.pipeline()
        responses = pipeline.execute()
        lines = print_key_value(keys, responses)
        for l in lines:
            file2.write(l)
            file2.write('\n')
