import threading
import time

# Create a Condition object
condition = threading.Condition()
shared_data = []

def producer():
    with condition: # Acquire the lock
        for i in range(5):
            item = f"item_{i}"
            shared_data.append(item)
            print(f"Producer produced: {item}")
            condition.notify() # Notify one waiting consumer
            time.sleep(0.1) # Simulate work

def consumer():
    with condition: # Acquire the lock
        while not shared_data: # Wait until data is available
            print("Consumer waiting for data...")
            condition.wait() # Release lock and wait
        item = shared_data.pop(0)
        print(f"Consumer consumed: {item}")

# Create and start threads
producer_thread = threading.Thread(target=producer)
consumer_thread = threading.Thread(target=consumer)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()