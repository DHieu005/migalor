import asyncio
from typing import Optional

import httpx as httpx
import migdalor
from migdalor.fanout import fan_out

from cluster.node.client import NodeClient
from cluster.node.logger import logger
from cluster.node.tasks import TaskManager


class FriendsManager:
    def __init__(self, node_address: migdalor.NodeAddress, cluster_address: migdalor.NodeAddress) -> None:
        self._cluster = migdalor.Cluster(
            node_address=(node_address),
            discovery=migdalor.KubernetesServiceDiscovery(service_address=cluster_address),
            nodes_added_handlers=[self._on_nodes_added],
            nodes_removed_handlers=[self._on_nodes_removed],
            update_every_secs=10,
        )

        self._greet_task: Optional[asyncio.Task] = None
        self._tasks = TaskManager(self._cluster)

    @property
    def current_node(self) -> migdalor.NodeAddress:
        return self._cluster.current_node

    @property
    def friends(self) -> list[migdalor.NodeAddress]:
        return self._cluster.other_nodes + [self._cluster.current_node]

    @property
    def tasks(self) -> TaskManager:
        return self._tasks

    async def greet_friends(self) -> None:
        await self._fan_out_to_peers(self._greet_peer)
        logger.info(f"({self._cluster.current_node}) greeted friend nodes ({self._cluster.other_nodes})")

    async def bye_friends(self) -> None:
        await self._fan_out_to_peers(self._bye_peer)
        logger.info(f"({self._cluster.current_node}) byed friend nodes ({self._cluster.other_nodes})")

    async def catchup(self) -> dict[str, str]:
        moods: dict[str, str] = {}

        async def fetch_mood(node: migdalor.NodeAddress) -> tuple[str, str]:
            client = NodeClient(node_ip=node[0], port=node[1])
            return node[0], await client.mood()

        fan_out_result = await fan_out(set(self._cluster.other_nodes), fetch_mood)

        for node, outcome in fan_out_result.failures.items():
            logger.warning(f"{self._cluster.current_node} {node} node was shut down already: {outcome}")

        for node_ip, mood in fan_out_result.successes.values():
            moods[node_ip] = mood

        return moods

    async def add_friend(self, node: migdalor.NodeAddress) -> None:
        await self._cluster.add(node)

    async def remove_friend(self, node: migdalor.NodeAddress) -> None:
        await self._cluster.remove(node)

    async def start(self) -> None:
        logger.info(f"({self._cluster.current_node}) node is starting up")

        self._greet_task = asyncio.create_task(self._greet_friends_in_background())
        await self._cluster.start()

    async def stop(self) -> None:
        logger.info(f"{self._cluster.current_node} node is shutting down")
        await self._cluster.stop()
        await self.bye_friends()

        if self._greet_task:
            self._greet_task.cancel()
            await self._greet_task

    async def _fan_out_to_peers(self, operation) -> None:
        async def call_peer(node: migdalor.NodeAddress) -> None:
            await operation(node)

        fan_out_result = await fan_out(set(self._cluster.other_nodes), call_peer)

        for node, outcome in fan_out_result.failures.items():
            if isinstance(outcome, httpx.ConnectError):
                logger.warning(
                    f"{self._cluster.current_node} could not reach friend node ({node}), "
                    f"the node is still starting up or already shut down"
                )
            else:
                logger.warning(f"{self._cluster.current_node} error on fan-out to {node}: {outcome}")

    async def _greet_peer(self, node: migdalor.NodeAddress) -> None:
        client = NodeClient(node_ip=node[0], port=node[1])
        await client.hey(current_node_address=self._cluster.current_node)

    async def _bye_peer(self, node: migdalor.NodeAddress) -> None:
        client = NodeClient(node_ip=node[0], port=node[1])
        await client.bye(current_node_address=self._cluster.current_node)

    async def _greet_friends_in_background(self) -> None:
        """
        Greet friends in background to unblock API server startup. Otherwise, it would end up in a deadlock
        """
        logger.info(f"{self._cluster.current_node} waiting for a friend list update")

        async with self._cluster.nodes_updated:
            await self._cluster.nodes_updated.wait()

        logger.info(f"{self._cluster.current_node} friend list updated")

        await self.greet_friends()

    async def _on_nodes_added(self, nodes: set[migdalor.NodeAddress]) -> None:
        logger.info(f"{self._cluster.current_node} nodes added ({nodes})")
        await self._fan_out_to_peers(self._greet_peer)

    async def _on_nodes_removed(self, nodes: set[migdalor.NodeAddress]) -> None:
        logger.info(f"{self._cluster.current_node} nodes removed ({nodes})")
