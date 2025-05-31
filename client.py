import asyncio
import websockets
import json

# Emoji mapping dictionary
emoji_mapping = {
    "😀": "Grinning Face",
    "❤": "Heart",
    "😭": "Loudly Crying Face",
    "😡": "Pouting Face",
    "😂": "Face with Tears of Joy"
}

# Function to generate emoji stream based on the scaled count
def generate_emoji_stream(emoji, count):
    # This will repeat the emoji based on the count
    return emoji * count

async def hello():
    uri = "ws://localhost:9001"  # Subscriber WebSocket server
    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()  # Receive real-time message
            #print(f"Received message: {message}")

            try:
                # Parse the message as JSON
                data = json.loads(message)  # Parse the JSON message
                emoji = data.get("emoji_type")  # Get the emoji character (e.g., "\ud83d\ude21")
                scaled_count = data.get("scaled_count", 0)  # Get the scaled count

                if emoji and scaled_count > 0:
                    # Directly map the emoji character to its name
                    emoji_name = emoji_mapping.get(emoji, "Unknown Emoji")
                    
                    # Generate the emoji string based on the scaled count
                    emoji_stream = generate_emoji_stream(emoji, scaled_count)

                    # Display the emoji stream and its name
                    print(f"Emoji Stream: {emoji_stream}")
                    # Optionally, send this emoji stream back to the client (if needed)
                    await websocket.send(emoji_stream)
                else:
                    print("Received invalid message format or empty scaled count.")
            except Exception as e:
                print(f"Error processing message: {e}")

# Start the client
asyncio.run(hello())
