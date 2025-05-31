from .defintion import SDG, DepEdge, DepKind, Indicator, PlaceKind, PlaceNode
from collections.abc import Callable


class MCPWrapper:
    def __init__(self, sdg: SDG):
        self._sdg = sdg

    def __getattr__(self, name):
        return getattr(self._sdg, name)

    def __dir__(self):
        return list(set(dir(self.__class__)) | set(dir(self._sdg)))

    def mcp_get_node_stat(self, node_id: int):
        raise NotImplementedError("This method is not implemented in MCPWrapper")

    def mcp_get_edge_stat(self, edge_id: int):
        """取决于这里的信息多吗；如果有用的话需要实现"""
        return NotImplementedError("This method is not implemented in MCPWrapper")

    def mcp_get_node_edges(self, node_id: int):
        """
        get all the edges of the node; 是为了允许沿着 node 做邻近节点的搜索
        如果 edge 信息有用，则 edgeid 也需要返回；否则只需要返回邻近的 node id
        """
        raise NotImplementedError("This method is not implemented in MCPWrapper")

    def mcp_get_paths(self, src_id: int, dst_id: int):
        """返回 node id 路径的列表应该就可以"""
        raise NotImplementedError("This method is not implemented in MCPWrapper")

    def mcp_get_suspicious_nodes(self, fn: Callable[[float, float], bool]):
        """
        fn 传入的是函数，左边是正常 node 值，右边是异常 node 值。值均为数字，llm 可以自定义阈值来定义什么是异常
        """
        raise NotImplementedError("This method is not implemented in MCPWrapper")
