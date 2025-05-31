from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import uuid
import logging
import time
import json
from kafka import KafkaProducer, KafkaConsumer
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_RAW = "emoji_topic"
KAFKA_TOPIC_PROCESSED = "emoji_processed_topic"

# Initialize Kafka Producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

# Subscribers and Clients Management
subscribers = {}
clients = {}

# Register client with a maximum connection limit of 5 per subscriber
@app.route('/register', methods=['POST'])
def register_client():
    data = request.json
    subscriber_id = data.get('subscriber_id')
    if not subscriber_id:
        return jsonify({"error": "subscriber_id is required"}), 400

    # Check if the subscriber already has the maximum allowed connections
    if len(subscribers.get(subscriber_id, [])) >= 5:
        return jsonify({"error": "Maximum connection limit reached for this subscriber"}), 403

    client_id = str(uuid.uuid4())
    if subscriber_id not in subscribers:
        subscribers[subscriber_id] = []
    subscribers[subscriber_id].append(client_id)
    clients[client_id] = subscriber_id

    logger.info(f"Client {client_id} registered under subscriber {subscriber_id}")
    return jsonify({
        "status": "success",
        "client_id": client_id,
        "message": "Client registered successfully"
    }), 200

@app.route('/unregister/<client_id>', methods=['POST'])
def unregister_client(client_id):
    subscriber_id = clients.get(client_id)
    if subscriber_id and client_id in subscribers.get(subscriber_id, []):
        subscribers[subscriber_id].remove(client_id)
        del clients[client_id]
        logger.info(f"Client {client_id} unregistered from subscriber {subscriber_id}")
        return jsonify({"status": "success", "message": "Client unregistered successfully"}), 200

    return jsonify({"error": "Client not registered"}), 400

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.json
    client_id = data.get('client_id')
    emoji_type = data.get('emoji_type')

    if client_id not in clients:
        return jsonify({"error": "Client not registered"}), 400

    message = {
        "client_id": client_id,
        "emoji_type": emoji_type,
        "timestamp": time.time()
    }

    try:
        kafka_producer.send(KAFKA_TOPIC_RAW, message)
        kafka_producer.flush()
        logger.info(f"Emoji sent to Kafka: {message}")
        return jsonify({"status": "success", "message": "Emoji sent to stream"}), 200
    except Exception as e:
        logger.error(f"Error sending emoji to Kafka: {e}")
        return jsonify({"error": "Failed to send emoji"}), 500

@socketio.on('connect')
def handle_connect():
    logger.info(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    logger.info(f"Client disconnected: {request.sid}")

def kafka_consumer_task():
    """Kafka Consumer that streams processed data to connected WebSocket clients."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC_PROCESSED,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:

        data = message.value
        subscriber_id = data.get("subscriber_id")  # Assuming processed data contains this field

        if subscriber_id and subscriber_id in subscribers:
            for client_id in subscribers[subscriber_id]:
                # Emit only JSON data
                socketio.emit('processed_data', data, room=client_id)
                logger.info(f"Sent data to client {client_id} of subscriber {subscriber_id}")

# Start Kafka Consumer in a separate thread
consumer_thread = threading.Thread(target=kafka_consumer_task)
consumer_thread.daemon = True
consumer_thread.start()

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "status": "running",
        "subscribers": subscribers,
        "clients": list(clients.keys())
    })

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5002, debug=True)
