import websocket
import json
import threading
import requests
import argparse
from termcolor import colored

class EmojiReceiver:
    def __init__(self, base_url, subscriber_id):
        self.base_url = base_url
        self.subscriber_id = subscriber_id
        self.client_id = self.register_client()
        self.websocket = None

    def register_client(self):
        """Register a client under a specific subscriber"""
        register_url = f"{self.base_url}/register"
        response = requests.post(register_url, json={"subscriber_id": self.subscriber_id})
        
        if response.status_code == 200:
            print(colored(f"Client registered successfully!", "green"))
            return response.json()['client_id']
        else:
            print(colored(f"Registration failed: {response.json().get('error', 'Unknown error')}", "red"))
            raise Exception("Client registration failed")

    def connect_websocket(self):
        """Connect to WebSocket for real-time emoji stream"""
        def on_message(ws, message):
            try:
                # Ignore non-JSON messages
                if not message.strip().startswith('{'):
                    print(colored(f"Ignoring non-JSON message: {message}", "yellow"))
                    return

                # Parse the entire message as JSON
                data = json.loads(message)
                self.display_emoji_data(data)
            except json.JSONDecodeError as e:
                print(colored(f"Error processing message (invalid JSON): {e}", "red"))
            except Exception as e:
                print(colored(f"Unexpected error processing message: {e}", "red"))

        def on_error(ws, error):
            print(colored(f"WebSocket Error: {error}", "red"))

        def on_close(ws, close_status_code, close_msg):
            print(colored("WebSocket connection closed", "yellow"))

        def on_open(ws):
            print(colored("WebSocket connection opened", "green"))

        # Use the WebSocket endpoint directly
        websocket_url = "ws://localhost:5002/socket.io/?EIO=4&transport=websocket"
        
        self.websocket = websocket.WebSocketApp(
            websocket_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        # Run WebSocket in a separate thread
        wst = threading.Thread(target=self.websocket.run_forever)
        wst.daemon = True
        wst.start()

    def display_emoji_data(self, data):
        """Display processed emoji data in a colorful CLI format"""
        if not isinstance(data, dict):
            return

        window = data.get('window', {})
        window_start = window.get('start')
        window_end = window.get('end')
        emoji_type = data.get('emoji_type', 'Unknown')
        count = data.get('emoji_count', 0)
        scaled_count = data.get('scaled_count', 1)

        color_map = {
            '😀': 'green', 
            '😂': 'yellow', 
            '❤': 'red', 
            '😭': 'blue', 
            '😡': 'magenta'
        }

        print(colored(
            f"📊 Emoji Insights [{window_start} - {window_end}]: "
            f"{emoji_type} x {count} (Scaled: {scaled_count})", 
            color_map.get(emoji_type, 'white')
        ))

    def unregister(self):
        """Unregister the client"""
        unregister_url = f"{self.base_url}/unregister/{self.client_id}"
        response = requests.post(unregister_url)
        
        if response.status_code == 200:
            print(colored("Client unregistered successfully!", "green"))
        else:
            print(colored("Unregistration failed", "red"))

def main():
    parser = argparse.ArgumentParser(description="Real-time Emoji Stream Receiver")
    parser.add_argument('--base_url', default='http://localhost:5002', help='Base URL for registration')
    parser.add_argument('--subscriber_id', default='default_subscriber', help='Subscriber ID')
    args = parser.parse_args()

    try:
        receiver = EmojiReceiver(args.base_url, args.subscriber_id)
        receiver.connect_websocket()

        # Keep the main thread running
        input(colored("\n🔴 Press Enter to stop the emoji receiver...\n", "cyan"))
        
        receiver.unregister()
    except KeyboardInterrupt:
        print("\nStopping emoji receiver...")
    except Exception as e:
        print(colored(f"Error: {e}", "red"))

if __name__ == "__main__":
    main()
