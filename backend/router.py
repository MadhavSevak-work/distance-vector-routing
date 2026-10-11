import json
import logging
import socket
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("Router")

INF = 999

class Router:
    """
    Router represents an independent network node running in its own thread.
    Each router:
      - Binds to a unique UDP port (e.g., 5001-5004).
      - Listens for incoming UDP datagrams containing neighbors' distance vectors.
      - Implements Bellman-Ford distance vector logic to compute shortest paths.
      - Maintains its routing table: Destination -> { cost, next_hop }
    """

    def __init__(
        self,
        node_id: str,
        port: int,
        on_table_changed: Optional[Callable[["Router", str, int, int, str], None]] = None,
        on_state_updated: Optional[Callable[[], None]] = None
    ):
        self.node_id = node_id.upper()
        self.port = port
        self.on_table_changed = on_table_changed
        self.on_state_updated = on_state_updated

        # Neighbor link costs: neighbor_id -> cost (INF if link is broken)
        # neighbor_ports: neighbor_id -> port
        self.neighbors: Dict[str, int] = {}
        self.neighbor_ports: Dict[str, int] = {}

        # Distance vector / routing table:
        # dest_id -> { "cost": int, "next_hop": str }
        self.routing_table: Dict[str, Dict[str, Any]] = {
            self.node_id: {"cost": 0, "next_hop": self.node_id}
        }

        # Stored vectors received from direct neighbors:
        # neighbor_id -> { dest_id: cost }
        self.neighbor_vectors: Dict[str, Dict[str, int]] = {}

        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def set_neighbor(self, neighbor_id: str, port: int, cost: int):
        """Add or update a direct neighbor link."""
        with self._lock:
            self.neighbors[neighbor_id] = cost
            self.neighbor_ports[neighbor_id] = port
            if neighbor_id not in self.routing_table:
                self.routing_table[neighbor_id] = {"cost": cost, "next_hop": neighbor_id}
            elif self.routing_table[neighbor_id]["cost"] > cost:
                self.routing_table[neighbor_id] = {"cost": cost, "next_hop": neighbor_id}

    def update_link_cost(self, neighbor_id: str, cost: int):
        """Update the link cost to an existing neighbor (e.g. sever link with INF)."""
        with self._lock:
            if neighbor_id in self.neighbors:
                self.neighbors[neighbor_id] = cost
                # Re-evaluate Bellman-Ford
                self._recalculate_routing_table()

    def remove_neighbor(self, neighbor_id: str):
        """Remove a neighbor connection."""
        with self._lock:
            self.neighbors.pop(neighbor_id, None)
            self.neighbor_ports.pop(neighbor_id, None)
            self.neighbor_vectors.pop(neighbor_id, None)
            self._recalculate_routing_table()

    def remove_destination(self, dest_id: str):
        """Remove a destination completely when a node is deleted from the network."""
        with self._lock:
            self.neighbors.pop(dest_id, None)
            self.neighbor_ports.pop(dest_id, None)
            self.neighbor_vectors.pop(dest_id, None)
            self.routing_table.pop(dest_id, None)
            self._recalculate_routing_table()

    def restore_table_snapshot(self, table_snapshot: Dict[str, Dict[str, Any]], neighbor_vectors: Optional[Dict[str, Dict[str, int]]] = None):
        """Restore routing table and neighbor vectors to a previous snapshot."""
        with self._lock:
            self.routing_table = {dest: dict(val) for dest, val in table_snapshot.items()}
            if neighbor_vectors is not None:
                self.neighbor_vectors = {nbr: dict(vec) for nbr, vec in neighbor_vectors.items()}

    def start(self):
        """Start UDP listener thread."""
        if self.running:
            return

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow immediate reuse of address/port
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", self.port))
        self.sock.settimeout(0.5)

        self.running = True
        self.thread = threading.Thread(target=self._listen_loop, daemon=True, name=f"Router-{self.node_id}")
        self.thread.start()
        logger.info(f"Router {self.node_id} started on UDP 127.0.0.1:{self.port}")

    def stop(self):
        """Stop UDP listener thread and close socket."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        logger.info(f"Router {self.node_id} stopped.")

    def _listen_loop(self):
        """Continuously listen for incoming UDP distance vectors."""
        while self.running:
            sock = self.sock
            if not sock:
                break
            try:
                data, addr = sock.recvfrom(4096)
                if not data:
                    continue
                message = json.loads(data.decode("utf-8"))
                self._process_message(message, addr)
            except socket.timeout:
                continue
            except json.JSONDecodeError as e:
                logger.warning(f"Router {self.node_id}: Invalid JSON received: {e}")
            except Exception as e:
                if self.running:
                    logger.error(f"Router {self.node_id} socket error: {e}")

    def _process_message(self, message: dict, addr: Tuple[str, int]):
        """
        Processes distance vector message:
        Format:
        {
            "sender": "B",
            "vector": { "A": 2, "B": 0, "C": 1, "D": 3 }
        }
        """
        sender = message.get("sender")
        vector = message.get("vector")

        if not sender or not isinstance(vector, dict):
            return

        with self._lock:
            # Only process vectors from known direct neighbors
            if sender not in self.neighbors:
                return

            self.neighbor_vectors[sender] = vector
            changed = self._recalculate_routing_table()

        if changed and self.on_state_updated:
            self.on_state_updated()

    def _recalculate_routing_table(self) -> bool:
        """
        Apply Bellman-Ford equation:
        D_x(y) = min_v { c(x, v) + D_v(y) }
        where:
          - c(x, v) is direct cost to neighbor v
          - D_v(y) is neighbor v's advertised cost to destination y
        Returns True if any table entry changed.
        """
        # Collect all known destinations across own table and received neighbor vectors
        all_destinations = set(self.routing_table.keys())
        all_destinations.add(self.node_id)
        for dest in self.neighbors.keys():
            all_destinations.add(dest)
        for vec in self.neighbor_vectors.values():
            for dest in vec.keys():
                all_destinations.add(dest)

        any_change = False

        for dest in all_destinations:
            if dest == self.node_id:
                # Distance to self is always 0
                if self.routing_table.get(self.node_id) != {"cost": 0, "next_hop": self.node_id}:
                    self.routing_table[self.node_id] = {"cost": 0, "next_hop": self.node_id}
                continue

            current_entry = self.routing_table.get(dest, {"cost": INF, "next_hop": "—"})
            old_cost = current_entry["cost"]
            old_next_hop = current_entry["next_hop"]

            min_cost = INF
            best_next_hop = "—"

            # Check direct link to neighbor if dest is a direct neighbor
            if dest in self.neighbors and self.neighbors[dest] < INF:
                min_cost = self.neighbors[dest]
                best_next_hop = dest

            # Evaluate paths through all active neighbors
            for nbr, link_cost in self.neighbors.items():
                if link_cost >= INF:
                    continue  # Severed or down link
                
                nbr_vector = self.neighbor_vectors.get(nbr, {})
                nbr_cost_to_dest = nbr_vector.get(dest, INF)

                if nbr_cost_to_dest < INF:
                    total_cost = link_cost + nbr_cost_to_dest
                    if total_cost < min_cost:
                        min_cost = total_cost
                        best_next_hop = nbr

            # Detect change
            if min_cost != old_cost or (min_cost < INF and best_next_hop != old_next_hop):
                any_change = True
                self.routing_table[dest] = {
                    "cost": min_cost,
                    "next_hop": best_next_hop
                }
                logger.info(
                    f"[Bellman-Ford] Router {self.node_id}: Route to {dest} updated: "
                    f"cost {old_cost} -> {min_cost} (via {best_next_hop})"
                )
                if self.on_table_changed:
                    self.on_table_changed(self, dest, old_cost, min_cost, best_next_hop)

        return any_change

    def broadcast_distance_vector(self):
        """
        Sends current distance vector via UDP to all known neighbors.
        """
        if not self.running or not self.sock:
            return

        sock = self.sock
        with self._lock:
            # Build payload of current distance vector
            vector = {dest: data["cost"] for dest, data in self.routing_table.items()}
            payload = json.dumps({
                "sender": self.node_id,
                "vector": vector,
                "timestamp": time.time()
            }).encode("utf-8")

            # Send to each direct neighbor's port
            for nbr_id, nbr_port in self.neighbor_ports.items():
                if self.neighbors.get(nbr_id, INF) < INF:
                    try:
                        sock.sendto(payload, ("127.0.0.1", nbr_port))
                    except Exception as e:
                        logger.error(f"Router {self.node_id}: Failed to send UDP to {nbr_id}:{nbr_port}: {e}")

    def get_table_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Returns a snapshot of the current routing table."""
        with self._lock:
            return {dest: dict(val) for dest, val in self.routing_table.items()}
