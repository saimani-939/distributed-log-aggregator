import asyncio
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from kafka import KafkaConsumer

connected_clients = set()
error_windows = {}
WINDOW_DURATION = 10.0  # Sliding window size: 10 seconds
ERROR_THRESHOLD = 5     # Alert when >= 5 errors in 10 seconds

def process_log(log_data):
    service = log_data.get("service", "unknown")
    level = log_data.get("level", "INFO")
    now = time.time()

    if level == "ERROR":
        if service not in error_windows:
            error_windows[service] = deque()

        # Add current timestamp
        error_windows[service].append(now)

        # Evict timestamps older than 10 seconds (Sliding Window)
        while error_windows[service] and (now - error_windows[service][0]) > WINDOW_DURATION:
            error_windows[service].popleft()

        error_count = len(error_windows[service])

        # Trigger alert if threshold is crossed
        if error_count >= ERROR_THRESHOLD:
            return {
                "type": "CRITICAL_ALERT",
                "service": service,
                "error_count": error_count,
                "window_seconds": int(WINDOW_DURATION),
                "last_error": log_data.get("message", "N/A"),
                "trace_id": log_data.get("trace_id", "N/A"),
                "timestamp": datetime.now().strftime("%H:%M:%S")
            }
    return None

async def kafka_consumer_worker():
    consumer = KafkaConsumer(
        'app-logs',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert-engine-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("🚨 Alert Engine Kafka Consumer active...")

    loop = asyncio.get_event_loop()
    while True:
        records = await loop.run_in_executor(None, consumer.poll, 200)
        for _, messages in records.items():
            for msg in messages:
                alert = process_log(msg.value)
                if alert and connected_clients:
                    payload = json.dumps(alert)
                    for client in list(connected_clients):
                        try:
                            await client.send_text(payload)
                        except Exception:
                            connected_clients.remove(client)
        await asyncio.sleep(0.01)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(kafka_consumer_worker())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# Allow all origins to avoid browser WebSocket handshake blocks
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    if os.path.exists("dashboard.html"):
        return FileResponse("dashboard.html")
    return HTMLResponse("<h1>dashboard.html not found in project folder</h1>", status_code=404)

@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f" Connected client. Total active: {len(connected_clients)}")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"❌ Client disconnected. Total active: {len(connected_clients)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)