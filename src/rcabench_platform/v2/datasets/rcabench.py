from typing import Any
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
