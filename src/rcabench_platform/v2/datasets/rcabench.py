from ..sources.convert import link_subset
from ..logging import logger, timeit

from collections import defaultdict
from typing import Any
import random
import math
import json
import re


DATAPACK_PATTERN = (
    r"(ts|ts\d)-(mysql|ts-rabbitmq|ts-ui-dashboard|ts-\w+-service|ts-\w+-\w+-service|ts-\w+-\w+-\w+-service)-(.+)-[^-]+"
)


def rcabench_get_service_name(datapack_name: str) -> str:
    m = re.match(DATAPACK_PATTERN, datapack_name)
    assert m is not None, f"Invalid datapack name: `{datapack_name}`"
    service_name: str = m.group(2)
    return service_name


FAULT_TYPES: list[str] = [
    "PodKill",
    "PodFailure",
    "ContainerKill",
    "MemoryStress",
    "CPUStress",
    "HTTPRequestAbort",
    "HTTPResponseAbort",
    "HTTPRequestDelay",
    "HTTPResponseDelay",
    "HTTPResponseReplaceBody",
    "HTTPResponsePatchBody",
    "HTTPRequestReplacePath",
    "HTTPRequestReplaceMethod",
    "HTTPResponseReplaceCode",
    "DNSError",
    "DNSRandom",
    "TimeSkew",
    "NetworkDelay",
    "NetworkLoss",
    "NetworkDuplicate",
    "NetworkCorrupt",
    "NetworkBandwidth",
    "NetworkPartition",
    "JVMLatency",
    "JVMReturn",
    "JVMException",
    "JVMGarbageCollector",
    "JVMCPUStress",
    "JVMMemoryStress",
    "JVMMySQLLatency",
    "JVMMySQLException",
]


def get_parent_resource_from_pod_name(pod_name: str) -> tuple[str | None, str | None, str | None]:
    """
    从 Pod 名称解析出父资源（Deployment + ReplicaSet 或 StatefulSet/DaemonSet）

    支持的父资源类型：
    - Deployment Pods: <deployment-name>-<replicaset-hash>-<pod-hash>
        → 返回 ("Deployment", deployment_name, replicaset_name)
    - StatefulSet Pods: <statefulset-name>-<ordinal>
        → 返回 ("StatefulSet", statefulset_name, None)
    - DaemonSet Pods: <daemonset-name>-<pod-hash>
        → 返回 ("DaemonSet", daemonset_name, None)
    - 其他情况返回 (None, None, None)

    Args:
        podname (str): Pod 名称

    Returns:
        tuple: (parent_type, parent_name, replicaset_name_if_applicable)
    """
    # Deployment Pod 格式: <deployment-name>-<replicaset-hash>-<pod-hash>
    # 例如: nginx-deployment-5c689d88bb-q7zvf
    deployment_pattern = r"^(?P<deploy>.+?)-(?P<rs_hash>[a-z0-9]{5,10})-(?P<pod_hash>[a-z0-9]{5})$"
    match = re.fullmatch(deployment_pattern, pod_name)
    if match:
        deployment_name = match.group("deploy")
        replicaset_name = f"{deployment_name}-{match.group('rs_hash')}"
        return ("Deployment", deployment_name, replicaset_name)

    # StatefulSet Pod 格式: <statefulset-name>-<ordinal>
    # 例如: web-0, mysql-1
    statefulset_pattern = r"^(?P<sts>.+)-(\d+)$"
    match = re.fullmatch(statefulset_pattern, pod_name)
    if match:
        return ("StatefulSet", match.group("sts"), None)

    # DaemonSet Pod 格式: <daemonset-name>-<pod-hash>
    # 例如: fluentd-elasticsearch-abcde
    daemonset_pattern = r"^(?P<ds>.+)-([a-z0-9]{5})$"
    match = re.fullmatch(daemonset_pattern, pod_name)
    if match:
        return ("DaemonSet", match.group("ds"), None)

    # 其他情况（如裸 Pod 或未知格式）
    return (None, None, None)


HTTP_REPLACE_METHODS: list[str] = [
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTIONS",
    "PATCH",
]

HTTP_REPLACE_BODY_TYPE: dict[int, str] = {
    0: "empty",
    1: "random",
}

JVM_MEM_TYPE: dict[int, str] = {
    1: "heap",
    2: "stack",
}

JVM_RETURN_TYPE: dict[int, str] = {
    1: "String",
    2: "Int",
}

JVM_RETURN_VALUE_OPT: dict[int, str] = {
    0: "Default",
    1: "Random",
}


def rcabench_fix_injection(injection: dict[str, Any]) -> None:
    injection["fault_type"] = FAULT_TYPES[injection["fault_type"]]

    injection["engine_config"] = json.loads(injection["engine_config"])

    display_config: dict[str, Any] = json.loads(injection["display_config"])
    rcabench_fix_injection_display_config(display_config)
    injection["display_config"] = display_config


def rcabench_fix_injection_display_config(display_config: dict[str, Any]) -> None:
    if (replace_method := display_config.get("replace_method")) is not None:
        if isinstance(replace_method, int):
            display_config["replace_method"] = HTTP_REPLACE_METHODS[replace_method]
        elif isinstance(replace_method, str):
            pass
        else:
            raise ValueError(f"Invalid replace_method type: {type(replace_method)}. Expected int or str.")

    replacements = [
        ("body_type", HTTP_REPLACE_BODY_TYPE),
        ("mem_type", JVM_MEM_TYPE),
        ("return_type", JVM_RETURN_TYPE),
        ("return_value_opt", JVM_RETURN_VALUE_OPT),
    ]

    for k, d in replacements:
        v = display_config.get(k)
        if v is None:
            continue
        display_config[k] = d[v]


@timeit(log_args={"ratio", "train_dataset_name", "test_dataset_name"})
def rcabench_split_train_test(
    datapacks: list[str],
    train_ratio: float,
    train_dataset_name: str,
    test_dataset_name: str,
    previous_train_datapacks: list[str],
    previous_test_datapacks: list[str],
):
    assert len(datapacks) > 0, "Datapacks list cannot be empty."
    assert 0 < train_ratio < 1, "Ratio must be between 0 and 1."

    assert train_dataset_name and test_dataset_name
    assert train_dataset_name != test_dataset_name

    prev_train_set = set(previous_train_datapacks)
    prev_test_set = set(previous_test_datapacks)
    additional_datapacks = set(datapacks) - prev_train_set - prev_test_set

    group_by_service: defaultdict[str, list[str]] = defaultdict(list)
    for datapack in additional_datapacks:
        service_name = rcabench_get_service_name(datapack)
        group_by_service[service_name].append(datapack)

    min_group_size = min(len(v) for v in group_by_service.values())
    logger.debug("min_group_size: {}", min_group_size)

    threshold = min_group_size
    while True:
        train_total = 0
        for service_datapacks in group_by_service.values():
            num_train = math.ceil(len(service_datapacks) * train_ratio)
            num_train = min(num_train, threshold)
            train_total += num_train

        target = len(additional_datapacks) * train_ratio

        logger.debug("threshold={} (train_total={}, target={})", threshold, train_total, target)

        if train_total >= target:
            break

        threshold += 1

    train_datapacks: list[str] = []
    test_datapacks: list[str] = []

    for service_name, service_datapacks in group_by_service.items():
        random.shuffle(service_datapacks)

        num_train = math.ceil(len(service_datapacks) * train_ratio)
        num_train = min(num_train, threshold)

        train_datapacks.extend(service_datapacks[:num_train])
        test_datapacks.extend(service_datapacks[num_train:])

    train_datapacks = list(prev_train_set) + train_datapacks
    test_datapacks = list(prev_test_set) + test_datapacks

    link_subset("rcabench", train_dataset_name, train_datapacks)
    link_subset("rcabench", test_dataset_name, test_datapacks)

    logger.info("train dataset: {} ({} datapacks)", train_dataset_name, len(train_datapacks))
    logger.info("test dataset: {} ({} datapacks)", test_dataset_name, len(test_datapacks))
