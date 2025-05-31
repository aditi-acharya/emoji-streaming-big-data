import os
import argparse
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import threading
import random
import time
import json
import signal
import sys

# Set up argument parser
parser = argparse.ArgumentParser(description="Kafka and Flask configuration")
parser.add_argument('--kafka_bootstrap_servers', default='localhost:9092', help='Kafka bootstrap servers')
parser.add_argument('--kafka_topic', default='emoji_topic3', help='Kafka topic name')
parser.add_argument('--flask_host', default='0.0.0.0', help='Flask host')
parser.add_argument('--flask_port', type=int, default=5000, help='Flask port')
parser.add_argument('--num_clients', type=int, default=100, help='Number of clients to simulate')
parser.add_argument('--messages_per_second', type=int, default=200, help='Messages per second per client')

# Parse the arguments
args = parser.parse_args()

# Load the values from args
KAFKA_BOOTSTRAP_SERVERS = args.kafka_bootstrap_servers
KAFKA_TOPIC = args.kafka_topic
FLASK_HOST = args.flask_host
FLASK_PORT = args.flask_port
NUM_CLIENTS = args.num_clients
MESSAGES_PER_SECOND = args.messages_per_second

# Set up Flask
app = Flask(__name__)

# Set up Kafka producer with dynamic configuration
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.json
    user_id = data.get('user_id')
    emoji_type = data.get('emoji_type')
    timestamp = data.get('timestamp')

    message = {
        'user_id': user_id,
        'emoji_type': emoji_type,
        'timestamp': timestamp
    }

    try:
        kafka_producer.send(KAFKA_TOPIC, message)
        kafka_producer.flush()

        print(f"Message sent to Kafka: {message}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        pass

    return jsonify({"status": "success", "message": "Emoji data sent to Kafka"}), 200

# Function to simulate a single client
def simulate_single_client(client_id, messages_per_second):
    interval = 1 / messages_per_second
    while True:
        for _ in range(messages_per_second):
            emoji_choices = ["😀", "😂", "❤", "😭", "😡"]
            # Introducing a weighted distribution: more "😀" emojis and fewer "😭"
            emoji_weights = [0.3, 0.1, 0.2, 0.1, 0.2]  # Modify as needed for varied distribution
            emoji_type = random.choices(emoji_choices, emoji_weights)[0]
            emoji_data = {
                "user_id": client_id,
                "emoji_type": emoji_type,
                "timestamp": time.time()
            }
            try:
                response = app.test_client().post('/send_emoji', json=emoji_data)
                print(f"Client {client_id}: {response.json}")
            except Exception as e:
                print(f"Error with client {client_id}: {e}")
            time.sleep(interval)
            
# Function to simulate multiple clients concurrently
def simulate_multiple_clients(num_clients, messages_per_second):
    threads = []
    for i in range(num_clients):
        client_id = f"client_{i + 1}"
        thread = threading.Thread(target=simulate_single_client, args=(client_id, messages_per_second))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

# Signal handler for graceful exit
def signal_handler(sig, frame):
    print('Exiting gracefully...')
    try:
        kafka_producer.flush()
        kafka_producer.close()
    except Exception as e:
        print(f"Error during shutdown: {e}")
    finally:
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Main entry point
if __name__ == '__main__':
    # Start Flask app in a separate thread
    flask_thread = threading.Thread(target=app.run, kwargs={'threaded': True, 'host': FLASK_HOST, 'port': FLASK_PORT})
    flask_thread.start()

    # Simulate clients based on command-line arguments
    try:
        simulate_multiple_clients(num_clients=NUM_CLIENTS, messages_per_second=MESSAGES_PER_SECOND)
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
