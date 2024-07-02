# encoding: utf-8
import argparse
import struct
import time
import traceback
from enum import Enum
from kubernetes import client, config

import redis
from kubernetes.client import ApiException


# feature1: update k8s pod/sts image
# usage: python3 update_image.py  update-image -z qcsh4c -k /home/deploy/.kube/config -r sts -n k8redis-1-2 -c k8redis-1 -i ubuntu:v9.9.9


def update_image(opts):
    if opts.image:
        check_image_format(opts.image)
    if opts.resource == Resource.Pod.value:
        PodModify().update_image(opts)
    elif opts.resource == Resource.StatefulSet.value:
        StsModify().update_image(opts)


def list_resource(opts):
    k8s_config = str.strip(opts.kubeconfig)
    k8s_context = str.strip(opts.context)
    k8s_resource = str.strip(opts.resource)
    namespace = str.strip(opts.namespace)
    label = str.strip(opts.label)
    zone = str.strip(opts.zone)

    if label == "":
        raise Exception(f"label {label} is empty")
    if "=" not in label:
        raise Exception(f"label {label} is invalid")

    k8s_client = get_k8s_client(zone, k8s_config, k8s_context, k8s_resource)
    ret = None
    if Resource.Pod.value == k8s_resource:
        ret = k8s_client.list_namespaced_pod(namespace=namespace, label_selector=label, resource_version=0, limit=1000)
    elif Resource.StatefulSet.value == k8s_resource:
        ret = k8s_client.list_namespaced_stateful_set(namespace=namespace, label_selector=label, resource_version=0,
                                                      limit=500)
    arr = []
    for item in ret.items:
        arr.append(item.metadata.name)
    print(f"list {k8s_resource} label: {label} result={','.join(arr)}")


class Zone(Enum):
    QC_SH4C = "qcsh4c"


# k8s resource
class Resource(Enum):
    Pod = "pod"
    Deploy = "deploy"
    StatefulSet = "sts"


def get_k8s_client(zone: str, config_file: str, context: str, resource: str):
    if Resource.StatefulSet.value == resource:
        return get_sts_client(zone, config_file, context)
    elif Resource.Pod.value == resource:
        return get_pod_client(zone, config_file, context)


def get_sts_client(zone: str, config_file: str, context: str):
    if context:
        config.load_kube_config(config_file=config_file, context=context)
    else:
        config.load_kube_config(config_file=config_file)
    v1 = client.AppsV1Api()
    return v1


def get_pod_client(zone: str, config_file: str, context: str):
    if context:
        config.load_kube_config(config_file=config_file, context=context)
    else:
        config.load_kube_config(config_file=config_file)
    v1 = client.CoreV1Api()
    return v1


class BaseModify:

    def name(self):
        return "base"

    def update_image(self, opts: argparse.Namespace):
        raise Exception("non-implementation")


class PodModify(BaseModify):
    def name(self):
        return "Pod"

    def update_image(self, opts: argparse.Namespace):
        pod_name = str.strip(opts.name)
        container_name = str.strip(opts.container)
        image = str.strip(opts.image)
        namespace = str.strip(opts.namespace)

        k8s_client = get_pod_client(str.strip(opts.zone), str.strip(opts.kubeconfig), str.strip(opts.context))
        if pod_name:
            for name in str.split(pod_name, ","):
                if name == "":
                    continue
                time.sleep(1)
                pod = k8s_client.read_namespaced_pod(name=str.strip(name), namespace=namespace)
                self.update_one_pod_image(k8s_client, pod, container_name, image)

    def update_one_pod_image(self, k8s_client, pod, container_name, image):
        print(f"start to modify pod {pod.metadata.name} image to {image} ")
        if len(pod.spec.containers) <= 1:
            raise Exception("forbidden to modify pod image, only 1 container")
        container_found = False
        for index in range(len(pod.spec.containers)):
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
        sts_name = str.strip(opts.name)
        image = str.strip(opts.image)
        container_name = str.strip(opts.container)
        namespace = str.strip(opts.namespace)

        k8s_client = get_sts_client(str.strip(opts.zone), str.strip(opts.kubeconfig), str.strip(opts.context))
        if sts_name:
            for name in str.split(sts_name, ","):
                if name == "":
                    continue
                sts = k8s_client.read_namespaced_stateful_set(name=str.strip(name), namespace=namespace)
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
    if ":" not in image:
        raise Exception(f"image {image} is invalid")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='manipulate pod/sts/deploy.')
    subparser = parser.add_subparsers(dest="subcmd", title="subcmd")

    update_image_cmd = subparser.add_parser("update-image", help="update image for pod/sts/deploy.")
    update_image_cmd.add_argument('-z', '--zone', type=str, choices=[zone.value for zone in Zone],
                                  help='select zone, example: qcsh4c',
                                  required=True)
    update_image_cmd.add_argument('-k', '--kubeconfig', type=str, help='k8s config file, example: ~/.kube/config',
                                  required=True)
    update_image_cmd.add_argument('-ctx', '--context', type=str, help='k8s context, example: k8s-cls', default='')
    update_image_cmd.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default')
    update_image_cmd.add_argument('-r', '--resource', type=str, choices=[resource.value for resource in Resource],
                                  help='k8s resource, example: pod',
                                  required=True)
    update_image_cmd.add_argument('-n', '--name', type=str, help='k8s resource name list, example: test1-pod,test2-pod')
    update_image_cmd.add_argument('-c', '--container', type=str, help='container name, example: test1-container',
                                  required=True)
    update_image_cmd.add_argument('-i', '--image', type=str, help='k8s image name, example: ubuntu:latest',
                                  required=True)
    update_image_cmd.set_defaults(func=update_image)

    list_resource_cmd = subparser.add_parser("list-resource", help="list resource by label")
    list_resource_cmd.add_argument('-z', '--zone', type=str, choices=[zone.value for zone in Zone],
                                   help='select zone, example: qcsh4c',
                                   required=True)
    list_resource_cmd.add_argument('-k', '--kubeconfig', type=str, help='k8s config file, example: ~/.kube/config',
                                   required=True)
    list_resource_cmd.add_argument('-ctx', '--context', type=str, help='k8s context, example: k8s-cls', default='')
    list_resource_cmd.add_argument('-ns', '--namespace', type=str, help='k8s namespace', default='default')
    list_resource_cmd.add_argument('-r', '--resource', type=str, choices=[resource.value for resource in Resource],
                                   help='k8s resource, example: pod',
                                   required=True)
    list_resource_cmd.add_argument('-l', '--label', type=str, help='k8s filter label, example: env=test')
    list_resource_cmd.set_defaults(func=list_resource)

    args = parser.parse_args()
    if not args.subcmd:
        parser.print_help()
    else:
        args.func(args)
