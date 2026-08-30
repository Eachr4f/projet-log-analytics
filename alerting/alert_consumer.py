import json
import time
import requests
from confluent_kafka import Consumer

import os
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'alert-consumer',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['logs-anomalies'])

last_alert_time = {}  # cooldown par service
COOLDOWN_SECONDS = 300  # 5 minutes

def send_slack_alert(anomaly):
    service = anomaly.get("service", "unknown")
    now = time.time()

    if service in last_alert_time and (now - last_alert_time[service]) < COOLDOWN_SECONDS:
        print(f"Alerte pour {service} ignorée (cooldown actif)")
        return

    message = {
        "text": (
            f"🚨 *Anomalie détectée*\n"
            f"*Service:* {service}\n"
            f"*Type:* {anomaly.get('anomaly_type')}\n"
            f"*Taux d'erreur:* {round(anomaly.get('error_rate', 0) * 100, 1)}%\n"
            f"*Logs sur la fenêtre:* {anomaly.get('total_logs')}\n"
            f"*Fenêtre:* {anomaly.get('window_start')} → {anomaly.get('window_end')}"
        )
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=message)
    if response.status_code == 200:
        last_alert_time[service] = now
        print(f"Alerte envoyée pour {service}")
    else:
        print(f"Erreur envoi Slack: {response.status_code} - {response.text}")

print("Démarrage du consumer d'alertes...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur Kafka: {msg.error()}")
            continue
        anomaly = json.loads(msg.value().decode('utf-8'))
        send_slack_alert(anomaly)
except KeyboardInterrupt:
    print("\nArrêt du consumer d'alertes...")
finally:
    consumer.close()
