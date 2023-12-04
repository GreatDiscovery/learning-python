# encoding: utf-8
import argparse
import traceback
from enum import Enum
from kubernetes import client, config

import redis
from kubernetes.client import ApiException


# 更新一个redis集群的镜像，包括pod/sts/deploy等资源
# usage: python3 update_image.py -z qcsh4c -k
# /home/deploy/.kube/config -r sts -n k8redis-1-2 -c k8redis-1 -i
# ubuntu:v9.9.9


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

    def get_pod_client(self, zone: str, config_file: str):
        config.load_kube_config(config_file=config_file)
        v1 = client.CoreV1Api()
        return v1

    def get_sts_client(self, zone: str, config_file: str):
        config.load_kube_config(config_file=config_file)
        v1 = client.AppsV1Api()
        return v1


class PodModify(BaseModify):
    def name(self):
        return "Pod"

    def update_image(self, opts: argparse.Namespace):
        pod_name = opts.name
        container_name = opts.container
        image = opts.image

        k8s_client = super().get_pod_client(opts.zone, opts.kubeconfig)
        if pod_name:
            for name in str.split(pod_name, ","):
                pod = k8s_client.read_namespaced_pod(name=name, namespace=opts.namespace)
                self.update_one_pod_image(k8s_client, pod, container_name, image)

    def update_one_pod_image(self, k8s_client, pod, container_name, image):
        print(f"start to modify pod {pod.metadata.name} image to {image} ")
        if len(pod.spec.containers) <= 1:
            raise Exception("forbidden to modify pod image, only 1 container")
        container_found = False
        for index in range(pod.spec.template.spec.containers):
            # don't modify main container's image
            if index == 0:
                continue
            container = pod.spec.template.containers[index]
            if container.name == container_name:
                container.image = image
                container_found = True
                break
        if container_found:
            k8s_client.patch_namespaced_pod(name=pod.metadata.name, namespace=pod.metadata.namespace, body=pod)
            print(f"succeed in modifying pod {pod.metadata.name} image to {image} ")
        else:
            raise Exception("container not found")


class StsModify(BaseModify):
    def name(self):
        return "StatefulSet"

    def update_image(self, opts: argparse.Namespace):
        sts_name = opts.name
        image = opts.image
        container_name = opts.container

        k8s_client = super().get_sts_client(opts.zone, opts.kubeconfig)
        if sts_name:
            for name in str.split(sts_name, ","):
                sts = k8s_client.read_namespaced_stateful_set(name=name, namespace=opts.namespace)
                self.update_one_sts_image(k8s_client, sts, container_name, image)

    def update_one_sts_image(self, k8s_client, sts, container_name, new_image):
        print(f"start to modify sts {sts.metadata.name} image to {new_image} ")
        container_found = False
        for container in sts.spec.template.spec.containers:
            if container.name == container_name:
                container.image = new_image
                container_found = True
                break
        if container_found:
            k8s_client.patch_namespaced_stateful_set(name=sts.metadata.name, namespace=sts.metadata.namespace, body=sts)
            print(f"succeed in modifying sts {sts.metadata.name} image to {new_image} ")
        else:
            raise Exception("container not found")


def check_image_format(image: str):
    if image == "":
        raise Exception("image is empty")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='update image for pod/sts/deploy.')
    parser.add_argument('-z', '--zone', type=str, choices=[zone.value for zone in Zone],
                        help='select zone, example: qcsh4c',
                        required=True)
    parser.add_argument('-k', '--kubeconfig', type=str, help='k8s config file, example: ~/.kube/config', required=True)
    parser.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default')
    parser.add_argument('-r', '--resource', type=str, choices=[resource.value for resource in Resource],
                        help='k8s resource, example: pod',
                        required=True)
    parser.add_argument('-n', '--name', type=str, help='k8s resource name, example: test1-pod,test2-pod')
    parser.add_argument('-c', '--container', type=str, help='container name, example: test1-container', required=True)
    # don't use label, not unique
    # parser.add_argument('-l', '--label', type=str, help='k8s filter label, example: env=test')
    parser.add_argument('-i', '--image', type=str, help='k8s image name, example: ubuntu:latest', required=True)
    args = parser.parse_args()

    if args.image:
        check_image_format(args.image)
    if args.resource == Resource.Pod.value:
        PodModify().update_image(args)
    elif args.resource == Resource.StatefulSet.value:
        StsModify().update_image(args)
