import os
import json
import uuid

from fastapi import FastAPI
from kafka import KafkaProducer

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    ),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/orders")
def create_order():
    order = {
        "order_id": str(uuid.uuid4()),
        "product_id": 1001,
        "quantity": 2
    }

    producer.send("order-events", {
        "event_type": "OrderCreated",
        "data": order
    }).get(timeout=10)

    return {
        "message": "Order created",
        "order": order
    }