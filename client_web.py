import websocket

def on_message(ws, message):
    print("Message from server:", message)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to WebSocket server")

url = "ws://localhost:5002/socket.io/?EIO=4&transport=websocket"
ws = websocket.WebSocketApp(url, 
                            on_message=on_message, 
                            on_error=on_error, 
                            on_close=on_close)
ws.on_open = on_open
ws.run_forever()
