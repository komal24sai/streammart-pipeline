import json, os
from confluent_kafka import Consumer, KafkaError  # type: ignore[attr-defined]
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from dotenv import load_dotenv

load_dotenv()

bq = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
table_id = f"{os.getenv('GCP_PROJECT_ID')}.streammart_raw.raw_orders"

def ensure_table():
    dataset_ref = bq.dataset('streammart_raw')
    try:
        bq.get_dataset(dataset_ref)
    except Exception:
        bq.create_dataset(dataset_ref)
        print("Created dataset streammart_raw")

    schema = [
        bigquery.SchemaField('order_id',   'STRING',    mode='REQUIRED'),
        bigquery.SchemaField('user_id',    'STRING',    mode='REQUIRED'),
        bigquery.SchemaField('product',    'STRING',    mode='REQUIRED'),
        bigquery.SchemaField('quantity',   'INTEGER',   mode='REQUIRED'),
        bigquery.SchemaField('price',      'FLOAT',     mode='REQUIRED'),
        bigquery.SchemaField('status',     'STRING',    mode='REQUIRED'),
        bigquery.SchemaField('order_date', 'TIMESTAMP', mode='REQUIRED'),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        bq.get_table(table_id)
    except Exception:
        bq.create_table(table)
        print(f"Created table {table_id}")

ensure_table()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'group.id': 'streammart-consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([os.getenv('KAFKA_TOPIC', 'orders')])

batch, BATCH_SIZE = [], 100

def flush_batch(batch):
    if not batch:
        return
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_APPEND)
    job = bq.load_table_from_json(batch, table_id, job_config=job_config)
    job.result()
    if job.errors:
        print(f"BQ errors: {job.errors}")
    else:
        print(f"Loaded {len(batch)} rows to BigQuery")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        err = msg.error()
        if err:
            if err.code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {err}")
            continue
        value = msg.value()
        if value is None:
            continue
        order = json.loads(value.decode('utf-8'))
        batch.append(order)
        if len(batch) >= BATCH_SIZE:
            flush_batch(batch)
            batch = []
finally:
    flush_batch(batch)
    consumer.close()