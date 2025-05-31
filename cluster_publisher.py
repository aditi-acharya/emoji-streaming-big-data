import asyncio
import websockets

# Store connected subscribers
subscribers = set()

# Handle messages from Main Publisher
async def handle_publisher(websocket, path=""):
    print("Main Publisher connected")
    try:
        async for message in websocket:
            print(f"Received from Main Publisher: {message}")
            # Forward message to all connected subscribers
            for subscriber in list(subscribers):  # Use a copy to avoid issues during iteration
                try:
                    await subscriber.send(message)
                    print(f"Forwarded message to subscriber {subscriber.remote_address}")
                except websockets.exceptions.ConnectionClosed:
                    subscribers.remove(subscriber)  # Remove disconnected subscriber
    except websockets.exceptions.ConnectionClosed:
        print("Main Publisher disconnected")

# Handle new subscriber connections
async def handle_subscriber(websocket, path=""):
    print(f"New subscriber connected: {websocket.remote_address}")
    subscribers.add(websocket)
    try:
        async for message in websocket:
            pass  # Subscribers are only receiving, no need to handle their messages
    except websockets.exceptions.ConnectionClosed:
        print(f"Subscriber disconnected: {websocket.remote_address}")
    finally:
        subscribers.remove(websocket)

# Start WebSocket server for both subscribers and publisher
async def start_cluster_publisher(port):
    # Subscribers listen on the port (e.g., 8001, 8003)
    subscriber_server = await websockets.serve(handle_subscriber, 'localhost', port)
    # Main Publisher listens on port + 1 (e.g., 8002, 8004)
    publisher_server = await websockets.serve(handle_publisher, 'localhost', port + 1)

    print(f"Cluster Publisher started: Subscribers on port {port}, Main Publisher on port {port + 1}")
    await asyncio.gather(subscriber_server.wait_closed(), publisher_server.wait_closed())

# Run all Cluster Publishers concurrently
async def main():
    cluster_ports = [8001, 8003]  # Example: two clusters, listening on 8001 and 8003 for subscribers
    await asyncio.gather(*(start_cluster_publisher(port) for port in cluster_ports))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Cluster Publisher terminated.")
#cluster publisher
