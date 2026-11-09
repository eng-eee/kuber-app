import os
import json
import threading

from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI()

def consume_orders():
    consumer = KafkaConsumer(
        "order-events",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092"
        ),
        auto_offset_reset="earliest",
        group_id="notification-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    for message in consumer:
        event = message.value

        print(
            "Sending notification for:",
            event,
            flush=True
        )

@app.on_event("startup")
def startup():
    threading.Thread(
        target=consume_orders,
        daemon=True
    ).start()

@app.get("/health")
def health():
    return {"status": "healthy"}