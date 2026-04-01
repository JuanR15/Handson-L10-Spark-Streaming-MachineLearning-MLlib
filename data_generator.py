import socket
import json
import time
import random
from datetime import datetime

HOST = "localhost"
PORT = 9999

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print("Streaming data to localhost:9999...")
conn, addr = server_socket.accept()
print(f"Client connected: {addr}")

while True:
    data = {
        "trip_id": random.randint(1000, 9999),
        "driver_id": random.randint(1, 5),
        "distance_km": round(random.uniform(1, 20), 2),
        "fare_amount": round(random.uniform(5, 50), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    conn.send((json.dumps(data) + "\n").encode("utf-8"))
    time.sleep(1)