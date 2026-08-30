import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

SERVICES = ["payment", "auth", "catalog", "shipping", "notification"]
LEVELS = ["ERROR", "ERROR", "ERROR", "WARN", "INFO"]
TOPIC = "logs-raw"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur d'envoi: {err}")

def generate_log():
    level = random.choice(LEVELS)
    http_code = 200 if level == "INFO" else random.choice([400, 404, 500, 503])
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": random.choice(SERVICES),
        "level": level,
        "message": fake.sentence(),
        "http_code": http_code,
        "latency_ms": round(random.uniform(10, 800), 2),
        "ip": fake.ipv4()
    }
    return log

if __name__ == "__main__":
    print("Démarrage du générateur de logs... (Ctrl+C pour arrêter)")
    try:
        while True:
            log = generate_log()
            producer.produce(
                TOPIC,
                value=json.dumps(log).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            print(f"Envoyé: {log['service']} - {log['level']}")
            time.sleep(random.uniform(0.1, 0.5))  # débit variable
    except KeyboardInterrupt:
        print("\nArrêt du générateur...")
        producer.flush()
