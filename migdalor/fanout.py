import asyncio
from typing import Awaitable, Callable, Generic, TypeVar

from migdalor.discovery import NodeAddress

T = TypeVar("T")


class FanOutResult(Generic[T]):
    """Results of a parallel fan-out across cluster nodes."""

    def __init__(self, results: dict[NodeAddress, T | BaseException]) -> None:
        self._results = results

    @property
    def successes(self) -> dict[NodeAddress, T]:
        return {node: result for node, result in self._results.items() if not isinstance(result, BaseException)}

    @property
    def failures(self) -> dict[NodeAddress, BaseException]:
        return {node: result for node, result in self._results.items() if isinstance(result, BaseException)}

    @property
    def all_succeeded(self) -> bool:
        return not self.failures


async def fan_out(
    nodes: set[NodeAddress],
    operation: Callable[[NodeAddress], Awaitable[T]],
    *,
    return_exceptions: bool = True,
) -> FanOutResult[T]:
    """
    Execute an async operation on multiple nodes in parallel.

    Each node is called concurrently via asyncio.gather. By default, exceptions
    from individual nodes are captured in FanOutResult.failures instead of
    propagating to the caller.
    """

    if not nodes:
        return FanOutResult({})

    node_list = list(nodes)
    outcomes = await asyncio.gather(
        *[operation(node) for node in node_list],
        return_exceptions=return_exceptions,
    )

    return FanOutResult(dict(zip(node_list, outcomes)))
