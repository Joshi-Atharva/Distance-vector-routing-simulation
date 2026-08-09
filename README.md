# Distance Vector Routing (DVR) Simulation

A multi-threaded Python simulation of the **Distance Vector Routing (DVR)** algorithm using the **Bellman-Ford** shortest-path approach. Each network node runs as an independent thread communicating via thread-safe message queues and synchronized via a barrier mechanism.

---

## Project Overview

Distance Vector Routing is a dynamic routing protocol where each router maintains a routing table containing the shortest known distance to every destination in the network and the next hop to reach it. Nodes periodically exchange their distance vectors with direct neighbors and update their local tables until convergence is reached.

### Key Architecture Highlights
- **Thread-per-Node Architecture**: Every network router runs concurrently as an instance of `threading.Thread`.
- **Asynchronous Link Simulation**: Inter-router communication occurs via per-node incoming `queue.Queue` channels.
- **Iteration Synchronization**: A shared `threading.Barrier` ensures synchronized per-round table printing and visual pacing across all nodes.
- **Zero External Dependencies**: Implemented entirely with Python's standard library (`threading`, `queue`, `time`, `copy`).

---

## Repository Structure

```
.
├── src/
│   └── dvr_sim.py           # Core multi-threaded DVR simulation program
├── data/
│   ├── input.txt            # Default 3-node topology input file
│   ├── output.txt           # Sample simulation output trace
│   └── tests.txt            # Extended multi-node topology test inputs
├── samples/
│   ├── sample_cv.py         # Demo: Python Condition Variables (`threading.Condition`)
│   ├── sample_locks.py      # Demo: Thread synchronization locks (`threading.Lock`)
│   └── sample_threading.py  # Demo: Basic multi-threading setup (`threading.Thread`)
├── .gitignore               # Comprehensive Git ignore rules
├── requirements.txt         # Project requirements & Python version compatibility
└── README.md                # Project overview and documentation
```

---

## Input File Format

Topology files define the graph structure for the simulation:

```text
3          <-- Number of nodes
A B C      <-- Space-separated list of node names
A B 5      <-- Edge format: <node1> <node2> <cost>
A C 2
EOF        <-- End of input marker
```

---

## Quick Start

### Requirements
- **Python 3.8+** (Standard library only; no `pip install` required)

### Running the Simulation

Run with the default topology (`data/input.txt`):
```bash
python3 src/dvr_sim.py
```

Run with a custom topology file:
```bash
python3 src/dvr_sim.py data/tests.txt
```

---

## Educational Concurrency Samples

The [`samples/`](./samples) directory contains standalone Python scripts demonstrating fundamental threading concepts used in this project:

- **`samples/sample_locks.py`**: Demonstrates thread safety and shared state protection using `Lock`.
- **`samples/sample_cv.py`**: Demonstrates Producer-Consumer synchronization using `Condition` variables.
- **`samples/sample_threading.py`**: Demonstrates spawning and joining concurrent worker threads.

---

## License
This project is open-source under the MIT License.
