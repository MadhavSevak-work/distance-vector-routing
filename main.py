import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from backend.simulation import SimulationController

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("FastAPI-Backend")

# Active WebSocket connections manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast_json(self, data: dict):
        if not self.active_connections:
            return
        dead_connections = []
        message_text = json.dumps(data)
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message_text)
            except Exception as e:
                logger.warning(f"Error broadcasting to client: {e}")
                dead_connections.append(connection)

        for dead in dead_connections:
            self.disconnect(dead)


manager = ConnectionManager()
sim_controller = SimulationController(broadcast_interval=3.0)
main_loop: Optional[asyncio.AbstractEventLoop] = None


def broadcast_state_threadsafe(state: dict):
    """Callback invoked from router/controller background threads."""
    global main_loop
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(manager.broadcast_json(state), main_loop)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()

    # Wire up state callback
    sim_controller.on_state_change = broadcast_state_threadsafe

    # Initialize 4 routers (A, B, C, D) in a loop and start 3-second broadcast background task
    sim_controller.initialize_default_topology()
    sim_controller.start_background_loop()
    logger.info("FastAPI DVR Simulator started. Routers listening on UDP ports 5001-5004.")

    yield

    # Clean shutdown
    sim_controller.stop_background_loop()
    sim_controller.stop_routers()
    logger.info("FastAPI DVR Simulator stopped.")


app = FastAPI(title="Distance Vector Routing Simulator API", lifespan=lifespan)

# Mount static files directory if exists, or serve index.html directly
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_path = os.path.join(BASE_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>Distance Vector Routing Simulator Backend</h1><p>index.html not found.</p>")


@app.get("/api/state")
async def get_state():
    """HTTP endpoint to retrieve current simulator state."""
    return sim_controller.get_state_snapshot()


@app.post("/api/step")
async def step_simulation():
    """HTTP endpoint to manually step forward."""
    return sim_controller.step_forward()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    # Immediately send the current state upon connection
    await websocket.send_text(json.dumps(sim_controller.get_state_snapshot()))

    try:
        while True:
            raw_text = await websocket.receive_text()
            try:
                msg = json.loads(raw_text)
            except Exception:
                continue

            action = msg.get("action")

            if action == "step":
                sim_controller.step_forward()
            elif action == "toggle":
                sim_controller.toggle_simulation()
            elif action == "break_link":
                u = msg.get("source") or msg.get("u")
                v = msg.get("target") or msg.get("v")
                if u and v:
                    sim_controller.break_or_restore_link(u, v)
            elif action == "add_node":
                node_id = msg.get("node_id")
                target = msg.get("target")
                cost = int(msg.get("cost", 3))
                if node_id and target:
                    sim_controller.add_node(node_id, target, cost)
            elif action == "reset":
                sim_controller.initialize_default_topology()
            elif action == "get_state":
                await websocket.send_text(json.dumps(sim_controller.get_state_snapshot()))

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
