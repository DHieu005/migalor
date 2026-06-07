from pydantic.main import BaseModel

import migdalor


class DiscoveryRequest(BaseModel):
    node: migdalor.NodeAddress


class NodeList(BaseModel):
    nodes: list[migdalor.NodeAddress]


class MoodResponse(BaseModel):
    mood: str


class CatchupResponse(BaseModel):
    moods: dict[str, str]


class TaskRequest(BaseModel):
    task_ids: list[str]


class TaskResponse(BaseModel):
    results: dict[str, str]
    processed_by: migdalor.NodeAddress
