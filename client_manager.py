# client_manager.py
from flask import Flask, request, jsonify
import threading
import uuid
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class Subscriber:
    def __init__(self, subscriber_id):
        self.subscriber_id = subscriber_id
        self.clients = {}
        logger.info(f"Subscriber {subscriber_id} initialized")

    def register_client(self, client_id, callback):
        if client_id not in self.clients:
            self.clients[client_id] = callback
            logger.info(f"Client {client_id} registered with subscriber {self.subscriber_id}")
            return True
        return False

    def unregister_client(self, client_id):
        if client_id in self.clients:
            del self.clients[client_id]
            logger.info(f"Client {client_id} unregistered from subscriber {self.subscriber_id}")
            return True
        return False

class ClientManager:
    def __init__(self):
        self.subscribers = {}
        self.clients = {}
        # Initialize default subscribers
        self._initialize_default_subscribers()
        logger.info("ClientManager initialized")

    def _initialize_default_subscribers(self):
        # Create 3 default subscribers
        for i in range(3):
            subscriber_id = f"subscriber_0_{i}"
            self.subscribers[subscriber_id] = Subscriber(subscriber_id)
            logger.info(f"Default subscriber {subscriber_id} created")

    def register_subscriber(self, subscriber):
        self.subscribers[subscriber.subscriber_id] = subscriber
        logger.info(f"Registered subscriber {subscriber.subscriber_id}")

    def register_client(self, subscriber_id):
        if subscriber_id not in self.subscribers:
            logger.error(f"Subscriber {subscriber_id} not found")
            return None
            
        client_id = str(uuid.uuid4())
        
        def callback(data):
            timestamp = datetime.now().isoformat()
            print(f"[{timestamp}] Client {client_id} received: {data}")
        
        subscriber = self.subscribers[subscriber_id]
        if subscriber.register_client(client_id, callback):
            self.clients[client_id] = subscriber_id
            logger.info(f"Registered client {client_id} with subscriber {subscriber_id}")
            return client_id
        return None

    def unregister_client(self, client_id):
        if client_id in self.clients:
            subscriber_id = self.clients[client_id]
            subscriber = self.subscribers.get(subscriber_id)
            if subscriber and subscriber.unregister_client(client_id):
                del self.clients[client_id]
                logger.info(f"Unregistered client {client_id}")
                return True
        return False

client_manager = ClientManager()

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        'status': 'running',
        'subscribers': list(client_manager.subscribers.keys()),
        'client_count': len(client_manager.clients)
    })

@app.route('/register', methods=['POST'])
def register_client():
    data = request.json
    subscriber_id = data.get('subscriber_id')
    
    if not subscriber_id:
        return jsonify({'error': 'subscriber_id is required'}), 400
        
    client_id = client_manager.register_client(subscriber_id)
    if client_id:
        return jsonify({
            'status': 'success',
            'client_id': client_id,
            'message': 'Client registered successfully'
        })
    return jsonify({'error': 'Registration failed'}), 400

@app.route('/unregister/<client_id>', methods=['POST'])
def unregister_client(client_id):
    if client_manager.unregister_client(client_id):
        return jsonify({
            'status': 'success',
            'message': 'Client unregistered successfully'
        })
    return jsonify({'error': 'Unregistration failed'}), 400

@app.route("/")
def home():
    return jsonify({
        'status': 'active',
        'available_subscribers': list(client_manager.subscribers.keys()),
        'registered_clients': len(client_manager.clients)
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
