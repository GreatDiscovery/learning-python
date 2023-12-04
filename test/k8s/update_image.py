# encoding: utf-8
import argparse
import traceback
from enum import Enum
from kubernetes import client, config

import redis
from kubernetes.client import ApiException


# 更新一个redis集群的镜像，包括pod/sts/deploy等资源
# require k8s config
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

    def get_pod_client(self, zone: str):
        if zone not in Zone:
            raise Exception(f"invalid zone={zone}")
        config.load_kube_config()
        v1 = client.CoreV1Api()
        return v1

    def get_sts_client(self, zone: str):
        if zone not in Zone:
            raise Exception(f"invalid zone={zone}")
        config.load_kube_config()
        v1 = client.AppsV1Api
        return v1


class PodModify(BaseModify):
    def name(self):
        return "Pod"

    def update_image(self, opts: argparse.Namespace):
        label_selector = opts.label
        pod_name = opts.name
        container_name = opts.container
        image = opts.image

        k8s_client = super().get_pod_client(opts.zone)
        if pod_name:
            pod = k8s_client.read_namespaced_pod(name=pod_name, namespace=opts.namespace)
            self.update_one_pod_image(k8s_client, pod, container_name, image)
        elif label_selector:
            pod_list = k8s_client.list_namespaced_pod(opts.namespace, label_selector=label_selector)
            for pod in pod_list:
                self.update_one_pod_image(k8s_client, pod, container_name, image)

    def update_one_pod_image(self, k8s_client, pod, container_name, image):
        print(f"start to modify pod {pod.metadata.name} image to {image} ")
        if len(pod.spec.containers) <= 1:
            raise Exception("forbidden to modify pod image, only 1 container")
        container_found = False
        for index in range(pod.spec.containers):
            # don't modify main container's image
            if index == 0:
                continue
            container = pod.spec.containers[index]
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
        label_selector = opts.label
        sts_name = opts.name
        image = opts.image
        container_name = opts.container

        k8s_client = super().get_sts_client(opts.zone)
        if sts_name:
            sts = k8s_client.read_namespaced_stateful_set(name=sts_name, namespace=opts.namespace)
            self.updaate_one_sts_image(k8s_client, sts, container_name, image)
        elif label_selector:
            sts_list = k8s_client.list_namespaced_stateful_set(namespace=opts.namespace, label_selector=label_selector)
            for sts in sts_list:
                self.updaate_one_sts_image(k8s_client, sts, container_name, image)

    def update_one_sts_image(self, k8s_client, sts, container_name, new_image):
        print(f"start to modify sts {sts.metadata.name} image to {new_image} ")
        container_found = False
        for container in sts.spec.containers:
            if container.name == container_name:
                container.image = new_image
                container_found = True
                break
        if container_found:
            k8s_client.patch_namespaced_stateful_set(name=sts.metadata.name, namespace=sts.metadata.namespace)
            print(f"succeed in modifying sts {sts.metadata.name} image to {new_image} ")
        else:
            raise Exception("container not found")


def check_k8s_labels(labels: str):
    if labels == "":
        raise Exception("labels is empty")
    # todo check format


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='update image for pod/sts/deploy.')
    parser.add_argument('-z', '--zone', type=Zone, choices=list(Zone), help='select zone, example: Zone.QC_SH4C',
                        required=True)
    parser.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default', required=True)
    parser.add_argument('-r', '--resource', type=Resource, choices=list(Resource),
                        help='k8s resource, example: Resource.Pod',
                        required=True)
    parser.add_argument('-i', '--image', type=str, help='k8s image name, example: ubuntu:latest', required=True)
    parser.add_argument('-k', '--k8s-config', type=str, help='k8s config file, example: ~/.kube/config', required=True)
    parser.add_argument('-n', '--name', type=str, help='k8s resource name, example: test1-pod')
    parser.add_argument('-c', '--container', type=str, help='container name, example: test1-container', required=True)
    parser.add_argument('-l', '--label', type=str, help='k8s filter label, example: env=test')
    args = parser.parse_args()

    if args.label:
        check_k8s_labels(args.label)
    if args.resource.value == Resource.Pod.value:
        PodModify().update_image(args)
    elif args.resource == Resource.StatefulSet.value:
        StsModify().update_image(args)
