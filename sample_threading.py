import threading
import time

def task(name, delay):
    print(f"Thread {name}: Starting...")
    time.sleep(delay)  # Simulate some work or I/O operation
    print(f"Thread {name}: Finishing.")

# Create thread objects
thread1 = threading.Thread(target=task, args=("One", 2))
thread2 = threading.Thread(target=task, args=("Two", 1))

# Start the threads
thread1.start()
thread2.start()

# Wait for threads to complete (optional, but good practice)
thread1.join()
thread2.join()

print("Main program finished.")
