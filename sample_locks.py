from threading import Lock, Thread

counter = 0
lock = Lock()

def increment_counter():
    global counter
    for _ in range(100000):
        lock.acquire()  # Acquire the lock before modifying the shared resource
        counter += 1
        lock.release()  # Release the lock after modifying the shared resource

threads = []
for _ in range(5):
    t = Thread(target=increment_counter)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(f"Final counter value: {counter}")
