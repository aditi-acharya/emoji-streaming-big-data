# Emoji Streaming Application for Big Data

This project is designed to stream and process emoji data in real-time using a scalable, distributed architecture. It leverages Python-based microservices and message queues to simulate and manage emoji events across a network of clients and servers.

## 🚀 Features

- **Real-Time Emoji Streaming**: Simulates continuous emoji data flow.
- **Distributed Client Management**: Handles multiple clients with unique IDs and names.
- **Publisher-Subscriber Model**: Utilizes a publish-subscribe mechanism for efficient data dissemination.
- **Web Interface**: Provides a web-based interface for client interactions.
- **Testing Suite**: Includes tools for testing and validating the streaming system.

## 🧰 Project Structure
 📁 emoji-streaming-big-data/ ├── app.py # Main application entry point ├── client.py # Client-side logic ├── client_manager.py # Manages client connections and states ├── client_register.py # Handles client registration processes ├── client_web.py # Web interface for clients ├── cluster_publisher.py # Publishes emoji data to the cluster ├── emoji_stream.py # Core streaming logic ├── main_publisher.py # Main publisher module ├── receive_emoji.py # Receives and processes incoming emoji data ├── subscriber.py # Subscribes to emoji data streams ├── testing.py # Testing and validation scripts └── README.md # Project documentation
