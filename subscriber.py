import asyncio
import websockets

# Address of the cluster publishers' WebSocket server (subscribers connect to these)
cluster_publisher_address_1 = "ws://localhost:8001"  # Cluster 1 publisher address
cluster_publisher_address_2 = "ws://localhost:8003"  # Cluster 2 publisher address


# Store connected clients for each subscriber
connected_clients_1a = set()  # Clients connected to Subscriber 1A
connected_clients_1b = set()  # Clients connected to Subscriber 1B
connected_clients_2a = set()  # Clients connected to Subscriber 2A
connected_clients_2b = set()  # Clients connected to Subscriber 2B

# Handle new client connections to Subscriber 1A
async def handle_client_1a(websocket, path=""):
    print(f"New client connected to Subscriber 1A: {websocket.remote_address}")
    connected_clients_1a.add(websocket)
    try:
        async for message in websocket:
            pass  # Clients only receive messages, no need to handle their messages
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected from Subscriber 1A: {websocket.remote_address}")
    finally:
        connected_clients_1a.remove(websocket)

# Handle new client connections to Subscriber 1B
async def handle_client_1b(websocket, path=""):
    print(f"New client connected to Subscriber 1B: {websocket.remote_address}")
    connected_clients_1b.add(websocket)
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected from Subscriber 1B: {websocket.remote_address}")
    finally:
        connected_clients_1b.remove(websocket)

# Handle new client connections to Subscriber 2A
async def handle_client_2a(websocket, path=""):
    print(f"New client connected to Subscriber 2A: {websocket.remote_address}")
    connected_clients_2a.add(websocket)
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected from Subscriber 2A: {websocket.remote_address}")
    finally:
        connected_clients_2a.remove(websocket)

# Handle new client connections to Subscriber 2B
async def handle_client_2b(websocket, path=""):
    print(f"New client connected to Subscriber 2B: {websocket.remote_address}")
    connected_clients_2b.add(websocket)
    try:
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected from Subscriber 2B: {websocket.remote_address}")
    finally:
        connected_clients_2b.remove(websocket)

# Listen for messages from Cluster 1 and forward them to clients
async def listen_to_cluster_publisher_1():
    async with websockets.connect(cluster_publisher_address_1) as websocket:
        print(f"Connected to cluster publisher 1 at {cluster_publisher_address_1}")
        while True:
            message = await websocket.recv()
            print(f"Received from cluster publisher 1: {message}")
            # Forward message to all clients of both Subscriber 1A and 1B
            for client in list(connected_clients_1a) + list(connected_clients_1b):
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    connected_clients_1a.remove(client) if client in connected_clients_1a else connected_clients_1b.remove(client)

# Listen for messages from Cluster 2 and forward them to clients
async def listen_to_cluster_publisher_2():
    async with websockets.connect(cluster_publisher_address_2) as websocket:
        print(f"Connected to cluster publisher 2 at {cluster_publisher_address_2}")
        while True:
            message = await websocket.recv()
            print(f"Received from cluster publisher 2: {message}")
            # Forward message to all clients of both Subscriber 2A and 2B
            for client in list(connected_clients_2a) + list(connected_clients_2b):
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    connected_clients_2a.remove(client) if client in connected_clients_2a else connected_clients_2b.remove(client)

# Start WebSocket server to handle client connections for Subscriber 1A
async def start_client_server_1a(port):
    server = await websockets.serve(handle_client_1a, "localhost", port)
    print(f"Client WebSocket server for Subscriber 1A started on port {port}")
    await server.wait_closed()

# Start WebSocket server to handle client connections for Subscriber 1B
async def start_client_server_1b(port):
    server = await websockets.serve(handle_client_1b, "localhost", port)
    print(f"Client WebSocket server for Subscriber 1B started on port {port}")
    await server.wait_closed()

# Start WebSocket server to handle client connections for Subscriber 2A
async def start_client_server_2a(port):
    server = await websockets.serve(handle_client_2a, "localhost", port)
    print(f"Client WebSocket server for Subscriber 2A started on port {port}")
    await server.wait_closed()

# Start WebSocket server to handle client connections for Subscriber 2B
async def start_client_server_2b(port):
    server = await websockets.serve(handle_client_2b, "localhost", port)
    print(f"Client WebSocket server for Subscriber 2B started on port {port}")
    await server.wait_closed()

# Main function to start all the servers concurrently
async def main():
    # Ports where clients connect to each subscriber
    client_server_port_1a = 9001  # Port for Subscriber 1A clients
    client_server_port_1b = 9002  # Port for Subscriber 1B clients
    client_server_port_2a = 9003  # Port for Subscriber 2A clients
    client_server_port_2b = 9004  # Port for Subscriber 2B clients

    await asyncio.gather(
        start_client_server_1a(client_server_port_1a),  # Server for Subscriber 1A clients
        start_client_server_1b(client_server_port_1b),  # Server for Subscriber 1B clients
        start_client_server_2a(client_server_port_2a),  # Server for Subscriber 2A clients
        start_client_server_2b(client_server_port_2b),  # Server for Subscriber 2B clients
        listen_to_cluster_publisher_1(),  # Connect to Cluster 1
        listen_to_cluster_publisher_2(),  # Connect to Cluster 2
    )

# Ensure the script runs correctly
if __name__ == "__main__":
    asyncio.run(main())
