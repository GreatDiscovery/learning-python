# encoding: utf-8
import argparse
from enum import Enum

import redis


# 更新一个redis集群的镜像，包括pod/sts/deploy等资源
# usage: python3


class Zone(Enum):
    QC_SH4C = "qcsh4c"



# k8s resource
class Resource(Enum):
    Pod = 1
    Deploy = 2
    StatefulSet = 3


def get_k8s_client(zone: str):
    if zone not in Zone:
        raise Exception(f"invalid zone={zone}")
    return ""


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='update image for pod/sts/deploy.')
    parser.add_argument('-z', '--zone', type=Zone, choices=list(Zone), help='select zone, example: Zone.QC_SH4C',
                        required=True)
    parser.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default', required=True)
    parser.add_argument('', type=Resource, choices=list(Resource), help='k8s resource, example: Resource.Pod',
                        required=True)
    parser.add_argument('-n', '--name', type=str, help='k8s resource name, example: test1-pod')
    parser.add_argument('-c', '--container', type=str, help='container name, example: test1-container')
    parser.add_argument('-i', '--image', type=str, help='k8s image name, example: ubuntu:latest')
    args = parser.parse_args()
