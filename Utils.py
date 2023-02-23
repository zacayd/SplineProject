import threading

shared_resource = []

# Create a Lock object
lock = threading.Lock()

def add_to_resource(item):
    # Acquire the lock to ensure exclusive access
    lock.acquire()

    try:
        # Perform the operation on the shared resource
        shared_resource.append(item)
    finally:
        # Release the lock to allow other threads to access the resource
        lock.release()
