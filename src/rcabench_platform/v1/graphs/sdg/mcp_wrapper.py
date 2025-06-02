from .defintion import SDG, DepEdge, DepKind, Indicator, PlaceKind, PlaceNode
from .statistics import STAT_PREFIX

from typing import Any, Literal
from collections.abc import Callable
from collections.abc import Iterable
from itertools import chain


class MCPWrapper:
    def __init__(self, sdg: SDG):
        self._sdg = sdg

    def __getattr__(self, name):
        return getattr(self._sdg, name)

    def __dir__(self):
        return list(set(dir(self.__class__)) | set(dir(self._sdg)))

    def mcp_get_node_stat(self, node_id: int):
        node = self._sdg.get_node_by_id(node_id)

        ans = {}
        for k, v in node.data.items():
            for prefix in STAT_PREFIX:
                if k.startswith(prefix):
                    ans[k] = v
                    break

        return ans

    def mcp_get_edge_stat(self, edge_id: int):
        edge = self._sdg.get_edge_by_id(edge_id)

        ans = {}
        for k, v in edge.data.items():
            for prefix in STAT_PREFIX:
                if k.startswith(prefix):
                    ans[k] = v
                    break

        return ans

    def mcp_get_node_edges(self, node_id: int, direction: Literal["in", "out", "both"] = "both"):
        iterables: list[Iterable[DepEdge]] = []
        if direction == "in" or direction == "both":
            iterables.append(self._sdg.in_edges(node_id))
        if direction == "out" or direction == "both":
            iterables.append(self._sdg.out_edges(node_id))

        ans: list[dict[str, Any]] = []
        for edge in chain(*iterables):
            ans.append(
                {
                    "id": edge.id,
                    "src_id": edge.src_id,
                    "dst_id": edge.dst_id,
                    "kind": edge.kind,
                }
            )

        return ans

    def mcp_get_paths(self, src_id: int, dst_id: int, directed: bool):
        return self._sdg.all_simple_paths(src_id, dst_id, directed=directed)

    def mcp_get_suspicious_nodes(self, attribute: str, fn: Callable[[float, float], bool]):
        """
        fn 传入的是函数，左边是正常 node 值，右边是异常 node 值。值均为数字，llm 可以自定义阈值来定义什么是异常
        """
        raise NotImplementedError("This method is not implemented in MCPWrapper")

    def mcp_get_avail_attributes(self):
        raise NotImplementedError("This method is not implemented in MCPWrapper")
