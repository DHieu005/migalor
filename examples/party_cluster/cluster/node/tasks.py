import asyncio

import httpx
import migdalor
from migdalor.fanout import fan_out
from migdalor.sharding import WorkSharder

from cluster.node.client import NodeClient
from cluster.node.logger import logger


class TaskManager:
    """
    Distributes work items across the cluster using WorkSharder and aggregates
    results from all owning nodes via parallel fan-out.
    """

    def __init__(self, cluster: migdalor.Cluster) -> None:
        self._cluster = cluster
        self._sharder = WorkSharder(cluster)

    @property
    def shard_index(self) -> int:
        return self._sharder.shard_index

    async def process_local(self, task_id: str) -> str:
        """Simulate CPU-bound work by hashing the task id on the owning node."""
        await asyncio.sleep(0.05)
        return f"processed:{task_id}@shard-{self.shard_index}"

    async def distribute(self, task_ids: list[str]) -> dict[str, str]:
        groups = self._sharder.group_by_owner(task_ids, key_fn=lambda task_id: task_id)
        results: dict[str, str] = {}

        local_tasks = groups.pop(self._cluster.current_node, [])
        for task_id in local_tasks:
            results[task_id] = await self.process_local(task_id)

        if not groups:
            return results

        async def execute_on_peer(node: migdalor.NodeAddress) -> dict[str, str]:
            peer_tasks = groups[node]
            client = NodeClient(node_ip=node[0], port=node[1])
            return await client.process_tasks(peer_tasks)

        fan_out_result = await fan_out(set(groups.keys()), execute_on_peer)

        for node, outcome in fan_out_result.failures.items():
            logger.warning(f"task fan-out failed for {node}: {outcome}")

        for peer_results in fan_out_result.successes.values():
            results.update(peer_results)

        return results
