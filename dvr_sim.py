# dvr_sim.py
import threading
import queue
import time
import copy

MAX_ITERS = 6
DELAY_BETWEEN_ROUNDS = 2.0   # seconds between rounds (as in many lab specs)

INF = 10**9

class Message:
    def __init__(self, src, vector, iteration):
        self.src = src
        self.vector = vector  # dict: dest -> cost
        self.iteration = iteration

class Node(threading.Thread):
    def __init__(self, name, neighbours, in_queues, all_nodes, print_barrier):
        super().__init__(name=str(name))
        self.name = name
        self.neighbours = neighbours  # dict neighbour -> cost (direct link cost)
        self.in_queue = in_queues[name]
        self.in_queues = in_queues    # reference to all in_queues to send messages
        self.all_nodes = all_nodes    # list of node names (for canonical ordering)
        self.print_barrier = print_barrier

        # routing table: dest -> (cost, next_hop)
        self.table = {}
        for n in all_nodes:
            if n == self.name:
                self.table[n] = (0, self.name)
            elif n in neighbours:
                self.table[n] = (neighbours[n], n)
            else:
                self.table[n] = (INF, None)

        # buffer for messages received this iteration
        self.recv_buffer = []  # list of Message objects
        self.iteration = 0
        self.stop_flag = threading.Event()

    def send_vector(self):
        # Build distance vector to send (dest -> cost)
        vector = {dest: cost for dest, (cost, nh) in self.table.items()}
        msg = Message(self.name, vector, self.iteration)
        # Send to all adjacent neighbours (simulate link)
        for nbr in self.neighbours:
            # put into neighbour's incoming queue
            self.in_queues[nbr].put(msg)

    def receive_from_neighbors(self):
        needed = len(self.neighbours)
        recvd = []
        # loop until we've collected 'needed' messages for current iteration
        while len(recvd) < needed:
            try:
                msg = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                # keep looping until we have expected messages
                continue
            # ignore old messages (from earlier iterations)
            if msg.iteration != self.iteration:
                # ignore and continue
                continue
            recvd.append(msg)
        self.recv_buffer = recvd

    def update_table(self):
        changed = False
        # Standard Bellman-Ford style updates from all received vectors
        for msg in self.recv_buffer:
            src = msg.src
            src_cost = self.neighbours[src]  # cost to reach the neighbour directly
            vector = msg.vector
            for dest, cost_to_dest_via_src in vector.items():
                if dest == self.name:
                    continue
                # candidate cost to dest via src
                cand = src_cost + cost_to_dest_via_src
                old_cost, old_nh = self.table[dest]
                if cand < old_cost:
                    self.table[dest] = (cand, src)
                    changed = True
        # clear recv_buffer for next iteration
        self.recv_buffer = []
        return changed

    def run(self):
        while not self.stop_flag.is_set() and self.iteration < MAX_ITERS:
            # Phase 1: send our current vector to all neighbours
            self.send_vector()

            # Phase 2: receive vectors from ALL neighbours for this iteration
            self.receive_from_neighbors()

            # Phase 3: update routing table based on received vectors
            changed = self.update_table()

            # Save whether any entry changed for printing asterisk
            self._last_changed = changed

            # Sync with other nodes and main thread for printing
            # All node threads plus main thread must wait on the same barrier.
            self.print_barrier.wait()

            # After printing, wait for main thread to allow next iteration start
            # (We can reuse the same barrier as a two-phase sync: main thread also waits again)
            self.print_barrier.wait()

            # proceed to next iteration
            self.iteration += 1

    def stop(self):
        self.stop_flag.set()


def pretty_table(node):
    rows = []
    for dest in sorted(node.table.keys()):
        cost, nh = node.table[dest]
        if cost >= INF:
            cost_str = "INF"
            nh_str = "-"
        else:
            cost_str = str(cost)
            nh_str = nh if nh is not None else "-"
        rows.append((dest, cost_str, nh_str))
    return rows

def print_all_tables(nodes, iteration):
    print(f"\n=== Iteration {iteration} routing tables ===")
    for n in sorted(nodes.keys()):
        node = nodes[n]
        rows = pretty_table(node)
        header = f"Node {n} (changed={'*' if getattr(node,'_last_changed',False) else ''})"
        print(header)
        print(" Dest | Cost | NextHop")
        for dest, cost, nh in rows:
            print(f"  {dest:4}  | {cost:4} | {nh}")
        print()

def run_simulation(topology, max_iters=MAX_ITERS, delay_between_rounds=DELAY_BETWEEN_ROUNDS):
    """
    topology: dict node -> dict(neighbour -> cost)
    e.g.
    topology = {
       'A': {'B':1, 'C':5},
       'B': {'A':1, 'C':2},
       'C': {'A':5, 'B':2}
    }
    """
    nodes_list = sorted(topology.keys())
    num_nodes = len(nodes_list)
    MAX_ITERS = max_iters

    # create per-node incoming queues
    in_queues = {n: queue.Queue() for n in nodes_list}

    # barrier: participants = num_nodes + 1 (main thread)
    print_barrier = threading.Barrier(parties=num_nodes + 1)

    # instantiate node threads
    node_objs = {}
    for n in nodes_list:
        node = Node(name=n, neighbours=topology[n], in_queues=in_queues,
                    all_nodes=nodes_list, print_barrier=print_barrier)
        node_objs[n] = node

    # start all nodes
    for node in node_objs.values():
        node.start()

    # main thread coordinates printing and per-round timing
    for it in range(MAX_ITERS):
        # Wait until all nodes reach the barrier after update
        print_barrier.wait()  # wait for nodes to finish update and arrive
        # Now main thread prints tables (nodes have _last_changed flag set)
        print_all_tables(node_objs, it)
        # optionally sleep for visual pacing / spec requirement
        time.sleep(delay_between_rounds)
        # let nodes continue to next iteration (second wait)
        print_barrier.wait()

    # after all iterations, stop node threads
    for node in node_objs.values():
        node.stop()

    # join threads - some nodes may be blocked waiting to receive messages for next iteration;
    # put dummy messages with iteration tag > current iteration to unblock if necessary.
    # We'll send terminating messages to each node's queue to avoid indefinite blocking.
    for n in nodes_list:
        # put dummy messages for neighbours count to unblock receive loops (iteration == last iter)
        for _ in range(len(topology[n])):
            in_queues[n].put(Message("TERM", {}, MAX_ITERS))

    for node in node_objs.values():
        node.join()

    print("\nSimulation finished.")

# Example topology and run
if __name__ == "__main__":
    # Example: A--1--B--2--C and A--5--C
    topology = {
        'A': {'B': 1, 'C': 5},
        'B': {'A': 1, 'C': 2},
        'C': {'A': 5, 'B': 2},
    }
    run_simulation(topology, max_iters=6, delay_between_rounds=2)

