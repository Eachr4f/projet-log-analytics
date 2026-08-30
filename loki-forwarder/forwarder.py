import json
import time
import requests
from confluent_kafka import Consumer

LOKI_URL = "http://localhost:3100/loki/api/v1/push"

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'loki-forwarder',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['logs-raw'])

def push_to_loki(log):
    timestamp_ns = str(int(time.time() * 1_000_000_000))
    payload = {
        "streams": [
            {
                "stream": {
                    "service": log.get("service", "unknown"),
                    "level": log.get("level", "unknown")
                },
                "values": [
                    [timestamp_ns, json.dumps(log)]
                ]
            }
        ]
    }
    try:
        response = requests.post(LOKI_URL, json=payload, timeout=5)
        if response.status_code != 204:
            print(f"Erreur Loki: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Erreur d'envoi vers Loki: {e}")

print("Démarrage du forwarder Kafka -> Loki...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur Kafka: {msg.error()}")
            continue
        log = json.loads(msg.value().decode('utf-8'))
        push_to_loki(log)
        print(f"Poussé vers Loki: {log['service']} - {log['level']}")
except KeyboardInterrupt:
    print("\nArrêt du forwarder...")
finally:
    consumer.close()
