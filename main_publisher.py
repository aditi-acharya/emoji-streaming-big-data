import asyncio
import websockets
import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "processed_emoji_topic"
KAFKA_BROKER = "localhost:9092"

# Cluster Publisher configurations
cluster_publishers = ["ws://localhost:8002", "ws://localhost:8004"]  # port + 1 for publishers

# Send a message to a specific cluster
async def send_to_cluster(cluster_address, message):
    try:
        async with websockets.connect(cluster_address) as websocket:
            await websocket.send(message)
            print(f"Sent to {cluster_address}: {message}")
    except Exception as e:
        print(f"Failed to send to {cluster_address}: {e}")

# Kafka consumer and message forwarding logic
async def kafka_consumer_handler():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Kafka Consumer connected.")

    # Consume messages from Kafka and forward to clusters
    for msg in consumer:
        message = json.dumps(msg.value)
        print(f"Received from Kafka: {message}")

        # Forward the message to all cluster publishers
        await asyncio.gather(*(send_to_cluster(cluster, message) for cluster in cluster_publishers))

# Main function
def main():
    print("Main Publisher started.")
    asyncio.run(kafka_consumer_handler())

if __name__ == "__main__":
    main()



#main publisher
