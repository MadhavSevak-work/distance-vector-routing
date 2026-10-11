import asyncio
import logging
import threading
import time
from typing import Dict, List, Optional, Callable, Any
from backend.router import Router, INF

logger = logging.getLogger("SimulationController")

class SimulationController:
    """
    Simulation Controller:
      - Initializes and manages Router instances.
      - Sets up the network topology and link costs.
      - Runs a background task that triggers periodic UDP broadcasts (every 3 seconds).
      - Tracks convergence and serializes full network state for WebSocket broadcasting.
    """

    def __init__(self, broadcast_interval: float = 3.0):
        self.broadcast_interval = broadcast_interval
        self.routers: Dict[str, Router] = {}
        self.edges: List[Dict[str, Any]] = []
        self.step_count = 0
        self.is_running = True
        self.converged = False
        self.latest_logs: List[str] = []
        
        # Callback invoked when state needs to be pushed via WebSocket
        self.on_state_change: Optional[Callable[[Dict[str, Any]], Any]] = None

        self._task_stop_event = threading.Event()
        self._bg_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Track changes during steps
        self._recent_changes: Dict[str, set] = {}
        self._history_stack: List[Dict[str, Any]] = []

    def _save_history_snapshot(self):
        """Save a deep copy snapshot of current state to the history stack."""
        snapshot = {
            "step_count": self.step_count,
            "edges": [dict(e) for e in self.edges],
            "converged": self.converged,
            "tables": {nid: r.get_table_snapshot() for nid, r in self.routers.items()},
            "neighbor_vectors": {
                nid: {nbr: dict(vec) for nbr, vec in r.neighbor_vectors.items()}
                for nid, r in self.routers.items()
            },
            "latest_logs": list(self.latest_logs)
        }
        self._history_stack.append(snapshot)
        if len(self._history_stack) > 50:
            self._history_stack.pop(0)

    def initialize_default_topology(self):
        """Initializes 4 routers (A, B, C, D) connected in a loop."""
        self.stop_routers()
        with self._lock:
            self.routers.clear()
            self.edges.clear()
            self._history_stack.clear()
            self.step_count = 0
            self.converged = False
            self.latest_logs = ["Initialized 4 routers (A, B, C, D) in a loop topology."]

            base_port = 5001
            nodes = [("A", 5001), ("B", 5002), ("C", 5003), ("D", 5004)]

            for node_id, port in nodes:
                router = Router(
                    node_id=node_id,
                    port=port,
                    on_table_changed=self._handle_table_changed,
                    on_state_updated=self._notify_state_update
                )
                self.routers[node_id] = router

            # Initial links in a loop: A-B(2), B-C(1), C-D(3), D-A(4)
            initial_links = [
                ("A", "B", 2),
                ("B", "C", 1),
                ("C", "D", 3),
                ("D", "A", 4)
            ]

            for u, v, cost in initial_links:
                self.routers[u].set_neighbor(v, self.routers[v].port, cost)
                self.routers[v].set_neighbor(u, self.routers[u].port, cost)
                self.edges.append({"from": u, "to": v, "cost": cost, "broken": False})

        # Start listening threads for each router
        for router in self.routers.values():
            router.start()

        self._notify_state_update()

    def _handle_table_changed(self, router: Router, dest: str, old_cost: int, new_cost: int, next_hop: str):
        log_entry = (
            f"Router {router.node_id}: Route to {dest} changed: "
            f"{'∞' if old_cost >= INF else old_cost} ➔ {'∞' if new_cost >= INF else new_cost} "
            f"(Next hop: {next_hop})"
        )
        with self._lock:
            self.converged = False
            self.latest_logs.append(log_entry)
            if len(self.latest_logs) > 50:
                self.latest_logs.pop(0)

            if router.node_id not in self._recent_changes:
                self._recent_changes[router.node_id] = set()
            self._recent_changes[router.node_id].add(dest)

    def _notify_state_update(self):
        """Invoke callback to push updated state to WebSocket clients."""
        if self.on_state_change:
            state = self.get_state_snapshot()
            try:
                self.on_state_change(state)
            except Exception as e:
                logger.error(f"Error in on_state_change callback: {e}")

    def step_forward(self) -> Dict[str, Any]:
        """
        Executes one discrete round of UDP distance vector broadcast from all routers.
        """
        with self._lock:
            self.step_count += 1
            self._recent_changes.clear()
            current_step = self.step_count

        # Each router broadcasts its vector to its neighbors via UDP
        routers_list = list(self.routers.values())
        for router in routers_list:
            router.broadcast_distance_vector()

        # Give small brief time for UDP datagrams to be received and processed by threads
        time.sleep(0.08)

        with self._lock:
            # If no changes were logged during this step, network has converged
            any_changes = sum(len(dest_set) for dest_set in self._recent_changes.values()) > 0
            if not any_changes and current_step > 1:
                self.converged = True
                self.latest_logs.append(f"Step {current_step}: No routing table changes. Network converged!")
            else:
                self.converged = False
                self.latest_logs.append(f"Step {current_step}: Distance vectors broadcasted over UDP.")

        self._notify_state_update()
        return self.get_state_snapshot()

    def start_background_loop(self):
        """Starts background worker thread broadcasting every 3 seconds."""
        if self._bg_thread and self._bg_thread.is_alive():
            return

        self._task_stop_event.clear()
        self._bg_thread = threading.Thread(target=self._background_worker, daemon=True, name="SimControllerLoop")
        self._bg_thread.start()
        logger.info(f"Background simulation loop started (interval={self.broadcast_interval}s).")

    def _background_worker(self):
        while not self._task_stop_event.is_set():
            if self.is_running:
                try:
                    self.step_forward()
                except Exception as e:
                    logger.error(f"Error in background simulation step: {e}")
            
            # Sleep in increments so stop event responds immediately
            for _ in range(int(self.broadcast_interval * 10)):
                if self._task_stop_event.is_set():
                    break
                time.sleep(0.1)

    def stop_background_loop(self):
        self._task_stop_event.set()
        if self._bg_thread and self._bg_thread.is_alive():
            self._bg_thread.join(timeout=1.0)

    def stop_routers(self):
        for router in self.routers.values():
            router.stop()

    def toggle_simulation(self) -> bool:
        """Pause or resume the 3-second automatic broadcast."""
        self.is_running = not self.is_running
        self._notify_state_update()
        return self.is_running

    def break_or_restore_link(self, u: str, v: str) -> bool:
        """Toggles link state between u and v (sever to INF or restore to original cost)."""
        u, v = u.upper(), v.upper()
        with self._lock:
            target_edge = None
            for edge in self.edges:
                if (edge["from"] == u and edge["to"] == v) or (edge["from"] == v and edge["to"] == u):
                    target_edge = edge
                    break

            if not target_edge:
                return False

            target_edge["broken"] = not target_edge["broken"]
            broken = target_edge["broken"]
            cost = INF if broken else target_edge["cost"]

        # Update link costs in both routers
        if u in self.routers and v in self.routers:
            self.routers[u].update_link_cost(v, cost)
            self.routers[v].update_link_cost(u, cost)

        state_msg = f"Severed link {u} ⟷ {v} (Cost = ∞)" if broken else f"Restored link {u} ⟷ {v} (Cost = {target_edge['cost']})"
        with self._lock:
            self.converged = False
            self.latest_logs.append(state_msg)

        self._notify_state_update()
        return True

    def add_node(self, node_id: str, connect_to: str, cost: int) -> bool:
        """Adds a new router connected to an existing node."""
        node_id = node_id.upper()
        connect_to = connect_to.upper()

        with self._lock:
            if node_id in self.routers or connect_to not in self.routers:
                return False

            # Assign next port
            existing_ports = [r.port for r in self.routers.values()]
            new_port = max(existing_ports, default=5000) + 1

            new_router = Router(
                node_id=node_id,
                port=new_port,
                on_table_changed=self._handle_table_changed,
                on_state_updated=self._notify_state_update
            )
            self.routers[node_id] = new_router

            # Connect with neighbor
            new_router.set_neighbor(connect_to, self.routers[connect_to].port, cost)
            self.routers[connect_to].set_neighbor(node_id, new_port, cost)

            self.edges.append({"from": node_id, "to": connect_to, "cost": cost, "broken": False})
            self.converged = False
            self.latest_logs.append(f"Added Router {node_id} (Port {new_port}) connected to {connect_to} with cost {cost}")

        new_router.start()
        self._notify_state_update()
        return True

    def get_state_snapshot(self) -> Dict[str, Any]:
        """Serializes current network topology and all router tables."""
        nodes_data = []
        routing_tables_data = {}

        all_node_ids = sorted(list(self.routers.keys()))

        for nid in all_node_ids:
            r = self.routers[nid]
            nodes_data.append({
                "id": nid,
                "port": r.port,
                "label": f"Node {nid}"
            })
            
            raw_table = r.get_table_snapshot()
            formatted_table = {}
            for dest in all_node_ids:
                entry = raw_table.get(dest, {"cost": INF, "next_hop": "—"})
                is_changed = (nid in self._recent_changes and dest in self._recent_changes[nid])
                formatted_table[dest] = {
                    "cost": entry["cost"],
                    "nextHop": entry["next_hop"],
                    "changed": is_changed
                }
            routing_tables_data[nid] = formatted_table

        return {
            "type": "state_update",
            "step": self.step_count,
            "running": self.is_running,
            "converged": self.converged,
            "nodes": nodes_data,
            "edges": list(self.edges),
            "routing_tables": routing_tables_data,
            "latest_log": self.latest_logs[-1] if self.latest_logs else "",
            "logs": self.latest_logs[-15:]
        }
