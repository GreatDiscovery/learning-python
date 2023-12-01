# encoding: utf-8
import argparse
import traceback
from enum import Enum
from kubernetes import client, config

import redis
from kubernetes.client import ApiException


# 更新一个redis集群的镜像，包括pod/sts/deploy等资源
# usage: python3


class Zone(Enum):
    QC_SH4C = "qcsh4c"



# k8s resource
class Resource(Enum):
    Pod = "pod"
    Deploy = "deploy"
    StatefulSet = "sts"


class BaseModify:

    def name(self):
        return "base"

    def update_image(self, opts: argparse.Namespace):
        raise Exception("non-implementation")

    def get_k8s_client(self, zone: str):
        if zone not in Zone:
            raise Exception(f"invalid zone={zone}")
        config.load_kube_config()
        v1 = client.CoreV1Api()
        return v1


class PodModify(BaseModify):
    def name(self):
        return "Pod"

    def update_image(self, opts: argparse.Namespace):
        k8s_client = super().get_k8s_client(opts.zone)
        label_selector = opts.label
        pod_name = opts.name
        if pod_name:
            try:
                pod = k8s_client.read_namespaced_pod(pod_name, opts.namespace)
                image = pod.spec.containers[0].image.split(":")[-1]
            except ApiException:
                print(f"get pod {pod_name} failed!")
                traceback.print_exc()
                return
        elif label_selector:
            ret = k8s_client.list_namespaced_pod(opts.namespace, label_selector=label_selector)
            for i in ret:
                image = i.spec.containers[0].image.split(":")[-1]


class StsModify(BaseModify):
    def name(self):
        return "StatefulSet"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='update image for pod/sts/deploy.')
    parser.add_argument('-z', '--zone', type=Zone, choices=list(Zone), help='select zone, example: Zone.QC_SH4C',
                        required=True)
    parser.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default', required=True)
    parser.add_argument('-r', '--resource', type=Resource, choices=list(Resource),
                        help='k8s resource, example: Resource.Pod',
                        required=True)
    parser.add_argument('-n', '--name', type=str, help='k8s resource name, example: test1-pod')
    parser.add_argument('-c', '--container', type=str, help='container name, example: test1-container')
    parser.add_argument('-i', '--image', type=str, help='k8s image name, example: ubuntu:latest', required=True)
    parser.add_argument('-l', '--label', type=str, help='k8s filter label, example: env=test')
    args = parser.parse_args()

    if args.resource.value == Resource.Pod.value:
        PodModify().update_image(args)
    elif args.resource == Resource.StatefulSet.value:
        StsModify().update_image(args)
