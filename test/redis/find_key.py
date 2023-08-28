# encoding: utf-8
import argparse

import redis

# 从redis集群中查找key位于哪个node上
# usage: python3 find_key.py --host=10.212.154.48 --key=mykey

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='find key in which node.')
    parser.add_argument('--host', type=str, help='redis cluster中随机一个节点的ip', required=True)
    parser.add_argument('-k', '--key', type=str, help='要查询的redis key', required=True)
    args = parser.parse_args()

    conn = redis.Redis(host=args.host, port=6379, db=0)
    """
        {'10.14.223.11:6379@16379': {'node_id': '053004d9a956b0d667a2bf3be88da126ac283e7c', 'flags': 'myself,master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '1693222365000', 'epoch': '4', 'slots': [['0', '5460']], 'connected': True}}
    """
    cluster_nodes = conn.cluster("nodes")
    slot_pos = conn.cluster("keyslot", args.key)
    for node in cluster_nodes:
        outer_arr = cluster_nodes[node]['slots']
        for inner_arr in outer_arr:
            if len(inner_arr) == 0:
                continue
            min_slot = int(inner_arr[0])
            max_slot = int(inner_arr[1])
            if max_slot > int(slot_pos) > min_slot:
                print(f'key={args.key} in node {node}')
