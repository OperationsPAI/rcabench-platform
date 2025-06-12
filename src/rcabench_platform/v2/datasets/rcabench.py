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
