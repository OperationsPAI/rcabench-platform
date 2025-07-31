import json
import math
import random
import re
from collections import defaultdict
from typing import Any

from ..logging import logger, timeit
from ..sources.convert import link_subset

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


def get_parent_resource_from_pod_name(
    pod_name: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Parse parent resource from Pod name (Deployment + ReplicaSet or StatefulSet/DaemonSet)

    Supported parent resource types:
    - Deployment Pods: <deployment-name>-<replicaset-hash>-<pod-hash>
        → Returns ("Deployment", deployment_name, replicaset_name)
    - StatefulSet Pods: <statefulset-name>-<ordinal>
        → Returns ("StatefulSet", statefulset_name, None)
    - DaemonSet Pods: <daemonset-name>-<pod-hash>
        → Returns ("DaemonSet", daemonset_name, None)
    - Other cases return (None, None, None)

    Args:
        podname (str): Pod name

    Returns:
        tuple: (parent_type, parent_name, replicaset_name_if_applicable)
    """
    # Deployment Pod format: <deployment-name>-<replicaset-hash>-<pod-hash>
    # Example: nginx-deployment-5c689d88bb-q7zvf
    deployment_pattern = r"^(?P<deploy>.+?)-(?P<rs_hash>[a-z0-9]{5,10})-(?P<pod_hash>[a-z0-9]{5})$"
    match = re.fullmatch(deployment_pattern, pod_name)
    if match:
        deployment_name = match.group("deploy")
        replicaset_name = f"{deployment_name}-{match.group('rs_hash')}"
        return ("Deployment", deployment_name, replicaset_name)

    # StatefulSet Pod format: <statefulset-name>-<ordinal>
    # Example: web-0, mysql-1
    statefulset_pattern = r"^(?P<sts>.+)-(\d+)$"
    match = re.fullmatch(statefulset_pattern, pod_name)
    if match:
        return ("StatefulSet", match.group("sts"), None)

    # DaemonSet Pod format: <daemonset-name>-<pod-hash>
    # Example: fluentd-elasticsearch-abcde
    daemonset_pattern = r"^(?P<ds>.+)-([a-z0-9]{5})$"
    match = re.fullmatch(daemonset_pattern, pod_name)
    if match:
        return ("DaemonSet", match.group("ds"), None)

    # Other cases (like bare Pod or unknown format)
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


@timeit(log_args={"train_ratio"})
def rcabench_split_train_test(
    datapacks: list[str],
    train_ratio: float,
    previous_datapacks: list[str],
    datapack_limit: int = 0,
):
    assert len(datapacks) > 0, "Datapacks list cannot be empty."
    assert 0 < train_ratio < 1, "Ratio must be between 0 and 1."
    assert datapack_limit <= len(datapacks), "Datapack limit must be less than or equal to the number of datapacks."

    prev_datapacks = set(previous_datapacks)
    additional_datapacks = set(datapacks) - prev_datapacks

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

    total_selected = len(train_datapacks) + len(test_datapacks)

    if total_selected > datapack_limit:
        target_train = int(datapack_limit * train_ratio)
        target_test = datapack_limit - target_train

        train_datapacks = train_datapacks[:target_train]
        test_datapacks = test_datapacks[:target_test]

        logger.info("Adjusted to datapack_limit: train={}, test={}", len(train_datapacks), len(test_datapacks))

    logger.info(
        "Final dataset: train={} datapacks, test={} datapacks, total={}",
        len(train_datapacks),
        len(test_datapacks),
        len(train_datapacks) + len(test_datapacks),
    )

    return train_datapacks, test_datapacks
