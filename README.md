# Distance Vector Routing Protocol Simulator 🌐

An interactive, real-time visualizer and simulation engine for the **Distance Vector Routing Protocol** (Bellman-Ford algorithm). Built with a distributed UDP-based Python backend, FastAPI WebSockets, and a modern frontend powered by Vis.js Network and Tailwind CSS.

---

## 🚀 Features

- **True Distributed Node Simulation**:
  - Independent virtual router instances running on separate threads.
  - Realistic inter-router communication using local **UDP sockets**.
  - Periodic asynchronous distance vector advertisements.

- **Routing Protocol Mechanics**:
  - Implementation of the **Bellman-Ford algorithm**.
  - Dynamic routing table updates and shortest path convergence.
  - Link cost updates and dynamic link failures/recoveries.
  - Support for **Split Horizon** and **Poison Reverse** to mitigate Count-to-Infinity loops.

- **Real-Time Interactive Visualization**:
  - Live network topology graph rendered with **Vis.js Network**.
  - Real-time routing table inspection for each node.
  - Live simulation event logger with packet exchange tracking.
  - Play, pause, step-by-step execution, and adjustable simulation speeds.
  - Live WebSocket communication between Python backend and browser UI.

---

## 🛠️ Tech Stack

- **Backend**:
  - Python 3.11+
  - [FastAPI](https://fastapi.tiangolo.com/) (REST API & WebSockets)
  - [Uvicorn](https://www.uvicorn.org/) (ASGI Server)
  - Python `socket` module (UDP message passing)
  - Python `threading` & `asyncio`
- **Frontend**:
  - HTML5 & JavaScript (ES6+)
  - [Vis.js Network](https://visjs.github.io/vis-network/docs/network/)
  - [Tailwind CSS](https://tailwindcss.com/)
  - Font Awesome Icons

---

## 📁 Project Structure

```plaintext
distance-vector-routing/
├── backend/
│   ├── router.py          # Virtual router node implementation (UDP + Bellman-Ford)
│   └── simulation.py      # Network simulation coordinator & topology controller
├── static/                # Static assets & frontend resources
├── index.html             # Interactive frontend UI & visualization dashboard
├── main.py                # FastAPI web server & WebSocket event broadcaster
├── run.py                 # Application launcher
├── run_server.bat         # Windows one-click start script
├── test_backend.py        # Automated test suite for routing & convergence
├── requirements.txt       # Project dependencies
└── README.md              # Project documentation
```

---

## ⚡ Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/MadhavSevak-work/distance-vector-routing.git
cd distance-vector-routing
```

### 2. Create and Activate a Virtual Environment
```bash
# Windows
python -m venv .venv
.\.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Simulator
```bash
python run.py
```
*(On Windows, you can also double-click `run_server.bat`)*

### 5. Open in Browser
Navigate to:
```
http://localhost:8000
```

---

## 🧪 Running Tests

To verify router convergence and Bellman-Ford calculations:
```bash
python test_backend.py
```

---

## 📝 License

This project is open source and available under the [MIT License](LICENSE).
