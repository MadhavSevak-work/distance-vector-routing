import time
import unittest
from backend.router import Router, INF
from backend.simulation import SimulationController

class TestDistanceVectorBackend(unittest.TestCase):
    def test_udp_routing_convergence(self):
        # Setup 4 routers on ports 5091-5094 to avoid collisions
        rA = Router("A", 5091)
        rB = Router("B", 5092)
        rC = Router("C", 5093)
        rD = Router("D", 5094)

        routers = [rA, rB, rC, rD]
        for r in routers:
            r.start()

        try:
            # Set up loop topology: A(2)B(1)C(3)D(4)A
            rA.set_neighbor("B", 5092, 2)
            rB.set_neighbor("A", 5091, 2)

            rB.set_neighbor("C", 5093, 1)
            rC.set_neighbor("B", 5092, 1)

            rC.set_neighbor("D", 5094, 3)
            rD.set_neighbor("C", 5093, 3)

            rD.set_neighbor("A", 5091, 4)
            rA.set_neighbor("D", 5094, 4)

            # Perform rounds of UDP broadcasting
            for _ in range(4):
                for r in routers:
                    r.broadcast_distance_vector()
                time.sleep(0.15)

            # Check convergence results for Router A
            tableA = rA.get_table_snapshot()
            
            # Destination A: cost 0
            self.assertEqual(tableA["A"]["cost"], 0)
            # Destination B: cost 2, next hop B
            self.assertEqual(tableA["B"]["cost"], 2)
            self.assertEqual(tableA["B"]["next_hop"], "B")
            # Destination C: path A->B->C is 2+1=3, while A->D->C is 4+3=7. So min cost is 3 via B
            self.assertEqual(tableA["C"]["cost"], 3)
            self.assertEqual(tableA["C"]["next_hop"], "B")
            # Destination D: direct is 4, path via B is 2+1+3=6. So min cost is 4 via D
            self.assertEqual(tableA["D"]["cost"], 4)
            self.assertEqual(tableA["D"]["next_hop"], "D")

            print("\n[SUCCESS] Router A table converged correctly via UDP:")
            for dest, info in tableA.items():
                print(f"  To {dest}: cost {info['cost']}, next_hop {info['next_hop']}")

        finally:
            for r in routers:
                r.stop()

    def test_simulation_controller(self):
        controller = SimulationController(broadcast_interval=1.0)
        controller.initialize_default_topology()
        try:
            state_initial = controller.get_state_snapshot()
            self.assertEqual(len(state_initial["nodes"]), 4)
            self.assertEqual(len(state_initial["edges"]), 4)

            # Step forward
            controller.step_forward()
            time.sleep(0.15)
            controller.step_forward()
            time.sleep(0.15)

            state_after = controller.get_state_snapshot()
            self.assertGreaterEqual(state_after["step"], 2)
            print("\n[SUCCESS] SimulationController step forward and snapshot passed.")
        finally:
            controller.stop_routers()

if __name__ == "__main__":
    unittest.main()
