"""
api/main.py
============
Topology Monitor backend (FastAPI). This is the integration point where
the database layer meets "any backend": it exposes the RocksDB state
store's data over REST + WebSocket for the React Flow dashboard, and can
be copy-pasted or mounted into a larger existing FastAPI/Django/Flask
service.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from database.rocksdb_store import RocksDBStateStore
from models.schemas import WindowedAggregate, WorkerHealth
from worker.partition_manager import PartitionManager

app = FastAPI(title="StreamForge Topology Monitor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # tighten this in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------- #
# Dependency: this is the reusable hook for integrating the database
# with ANY backend framework -- just call get_store() wherever you need
# read/write access to truck state.
# ---------------------------------------------------------------------- #
def get_store() -> RocksDBStateStore:
    if not hasattr(get_store, "_instance"):
        get_store._instance = RocksDBStateStore(
            db_path=f"{settings.rocksdb_path}-monitor-readonly",
            changelog_topic=None,  # monitor is read-focused; workers own writes
        )
    return get_store._instance


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/trucks/{truck_id}/current-average", response_model=WindowedAggregate | None)
def current_average(truck_id: str):
    """Read the truck's current 5-minute rolling average out of RocksDB."""
    store = get_store()
    for key, value in store.scan(prefix=f"truck:{truck_id}:window:"):
        return WindowedAggregate(
            truck_id=value["truck_id"],
            window_start=value["window_start"],
            window_end=value["window_end"],
            avg_temperature_c=round(value["sum_temperature_c"] / value["sample_count"], 3),
            sample_count=value["sample_count"],
            min_temperature_c=value["min_temperature_c"],
            max_temperature_c=value["max_temperature_c"],
        )
    return None


@app.get("/workers", response_model=list[WorkerHealth])
def list_workers():
    """Feeds the React Flow node health panel."""
    return PartitionManager.all_workers()


@app.post("/workers/{worker_id}/heartbeat")
def worker_heartbeat(worker_id: str, assigned_partitions: list[int], events_processed: int = 0):
    """Workers (Faust processes) call this periodically to report liveness."""
    PartitionManager.heartbeat(worker_id, assigned_partitions, events_processed)
    return {"status": "recorded"}


@app.websocket("/ws/topology")
async def topology_feed(websocket: WebSocket):
    """
    Live feed for the React Flow DAG: worker nodes, partition assignment,
    and throughput, pushed every second.
    """
    await websocket.accept()
    try:
        while True:
            payload = {
                "workers": [w.model_dump(mode="json") for w in PartitionManager.all_workers()],
                "num_partitions": settings.num_partitions,
            }
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
