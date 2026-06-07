import zlib
from typing import Callable, TypeVar

from migdalor.cluster import Cluster
from migdalor.discovery import NodeAddress

T = TypeVar("T")


def _stable_hash(key: str) -> int:
    return zlib.crc32(key.encode()) & 0xFFFFFFFF


class WorkSharder:
    """
    Partitions work items across cluster nodes using deterministic hashing.

    When membership changes, ownership is recalculated from the current sorted
    node list so each item is assigned to exactly one node in the cluster.
    """

    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster

    @property
    def node_count(self) -> int:
        return len(self._sorted_nodes())

    @property
    def shard_index(self) -> int:
        """Index of the current node in the sorted cluster membership."""
        return self._sorted_nodes().index(self._cluster.current_node)

    def owner_for(self, item_key: str) -> NodeAddress:
        nodes = self._sorted_nodes()
        if not nodes:
            return self._cluster.current_node

        return nodes[_stable_hash(item_key) % len(nodes)]

    def owns(self, item_key: str) -> bool:
        return self.owner_for(item_key) == self._cluster.current_node

    def partition(self, items: list[T], key_fn: Callable[[T], str]) -> list[T]:
        """Return only the items owned by the current node."""
        return [item for item in items if self.owns(key_fn(item))]

    def group_by_owner(self, items: list[T], key_fn: Callable[[T], str]) -> dict[NodeAddress, list[T]]:
        """Group items by the node that should process each one."""
        groups: dict[NodeAddress, list[T]] = {}

        for item in items:
            owner = self.owner_for(key_fn(item))
            groups.setdefault(owner, []).append(item)

        return groups

    def _sorted_nodes(self) -> list[NodeAddress]:
        return sorted([self._cluster.current_node, *self._cluster.other_nodes])
