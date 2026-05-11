import json, time, uuid
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer  # type: ignore[attr-defined]
from dotenv import load_dotenv
import os, random
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
if not bootstrap_servers:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set. Copy .env.example to .env and fill in the values.")

print("Bootstrap servers:", bootstrap_servers)
fake = Faker()
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

PRODUCTS = ['laptop', 'phone', 'headphones','tablet','Monitor'] 
STATUSES = ['placed','processing','shipped','delivered','cancelled']

def generate_order():
    order = {
        'order_id': str(uuid.uuid4()),
        'user_id': str(uuid.uuid4()),
        'product': random.choice(PRODUCTS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10.0, 999.9), 2),
        'status': random.choice(STATUSES),
        'order_date': datetime.now().isoformat()
    }
    return order

topic = os.getenv('KAFKA_TOPIC','orders')

try:
    while True:
        order = generate_order()
        producer.produce(topic, json.dumps(order).encode('utf-8'))
        print(f"Produced order: {order}")
        producer.flush()
        time.sleep(1)
finally:
    producer.flush()