from contextlib import asynccontextmanager
from fastapi import FastAPI, status

from cluster.node.entities import DiscoveryRequest, NodeList, MoodResponse, CatchupResponse, TaskRequest, TaskResponse
from cluster.node.friends import FriendsManager
from cluster.node.config import config
from cluster.node.mood import MoodManager

mood = MoodManager()
friends = FriendsManager(
    node_address=(config.node_ip, config.port),
    cluster_address=(config.cluster_hostname, config.port),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await friends.start()
    await mood.start()
    yield
    await friends.stop()
    await mood.stop()


app = FastAPI(
    title="The Party Cluster",
    description="An example of using Migdalor to implement peer discovery in Kubernetes cluster",
    lifespan=lifespan,
)

@app.post("/hey/", status_code=status.HTTP_202_ACCEPTED)
async def hey_from_other_node(request: DiscoveryRequest) -> None:
    await friends.add_friend(request.node)


@app.get("/catchUp/", status_code=status.HTTP_202_ACCEPTED)
async def catchup_with_all_nodes() -> CatchupResponse:
    return CatchupResponse(moods=await friends.catchup())


@app.post("/bye/", status_code=status.HTTP_202_ACCEPTED)
async def bye_from_other_node(request: DiscoveryRequest) -> None:
    await friends.remove_friend(request.node)


@app.get("/mood/", status_code=status.HTTP_200_OK)
async def get_mood() -> MoodResponse:
    return MoodResponse(mood=mood.current_mood)


@app.get("/friends/")
async def list_all_active_nodes() -> NodeList:
    return NodeList(
        nodes=friends.friends,
    )


@app.post("/tasks/distribute/", status_code=status.HTTP_200_OK)
async def distribute_tasks(request: TaskRequest) -> TaskResponse:
    results = await friends.tasks.distribute(request.task_ids)
    return TaskResponse(results=results, processed_by=friends.current_node)


@app.post("/tasks/process/", status_code=status.HTTP_200_OK)
async def process_tasks_locally(request: TaskRequest) -> TaskResponse:
    results = {task_id: await friends.tasks.process_local(task_id) for task_id in request.task_ids}
    return TaskResponse(results=results, processed_by=friends.current_node)
