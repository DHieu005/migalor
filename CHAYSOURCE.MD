Mở terminal paste dòng này vào chạy test 2 chuc nang

python -c @"
import asyncio
from migdalor import Cluster, StaticDiscovery, WorkSharder, fan_out
async def main(): # Giả lập cluster 3 node
cluster = Cluster(
('10.0.0.1', 8888),
StaticDiscovery([('10.0.0.1', 8888), ('10.0.0.2', 8888), ('10.0.0.3', 8888)]),
)
await cluster.\_update_nodes()
sharder = WorkSharder(cluster)
tasks = [f'task-{i}' for i in range(1, 11)]
print('=== WorkSharder ===')
print('Node hien tai:', cluster.current_node)
print('Shard index:', sharder.shard_index)
print('So node:', sharder.node_count)
groups = sharder.group_by_owner(tasks, key_fn=lambda t: t)
for node, node_tasks in sorted(groups.items()):
print(f' {node[0]} -> {node_tasks}')
local = sharder.partition(tasks, key_fn=lambda t: t)
print('Task local node xu ly:', local)
print()
print('=== fan_out ===')
async def ping(node):
await asyncio.sleep(0.1)
return f'pong from {node[0]}'
result = await fan_out(set(cluster.other_nodes), ping)
print('Thanh cong:', result.successes)
print('That bai:', result.failures)
asyncio.run(main())
"@
