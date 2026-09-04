# Plateforme d'analyse de logs à grande échelle

Pipeline complet de collecte, traitement, détection d'anomalies et visualisation
de logs applicatifs, avec intégration cloud hybride.

## Architecture

Génération → Kafka → Spark Streaming → (Stockage S3 + Indexation Loki) → Grafana + Alerting Slack
Orchestration batch (Airflow) + Infrastructure as Code (Terraform) + CI/CD (GitHub Actions)

## Stack technique

- **Ingestion**: Apache Kafka (mode KRaft)
- **Traitement temps réel**: Apache Spark Streaming (PySpark 4.2.0)
- **Stockage**: AWS S3 (format Parquet)
- **Indexation/recherche**: Grafana Loki
- **Visualisation**: Grafana (3 dashboards)
- **Alerting**: Slack (webhook + cooldown)
- **Orchestration batch**: Apache Airflow (2 DAGs)
- **Infrastructure**: Terraform
- **CI/CD**: GitHub Actions

## Prérequis

- Docker + Docker Compose
- Python 3.11+
- Java 17 (via SDKMAN recommandé)
- Compte AWS avec accès S3
- Compte Slack avec un webhook configuré

## Installation et lancement

### 1. Infrastructure Docker (Kafka, Loki, Grafana)
\`\`\`bash
docker compose up -d
\`\`\`

### 2. Générateur de logs
\`\`\`bash
cd generator
source venv/bin/activate
python3 generator.py
\`\`\`

### 3. Traitement Spark
\`\`\`bash
cd processing
source venv/bin/activate
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0 \
  process_logs.py
\`\`\`

### 4. Forwarder Loki
\`\`\`bash
cd loki-forwarder
source venv/bin/activate
python3 forwarder.py
\`\`\`

### 5. Consumer d'alertes
\`\`\`bash
cd alerting
source venv/bin/activate
export SLACK_WEBHOOK_URL="ton-url-webhook"
python3 alert_consumer.py
\`\`\`

### 6. Orchestration Airflow
\`\`\`bash
cd airflow
docker compose up -d
\`\`\`
Accès: http://localhost:8081

### 7. Infrastructure cloud (Terraform)
\`\`\`bash
cd terraform
terraform init
terraform apply
\`\`\`

## Accès aux interfaces

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Airflow | http://localhost:8081 |

## Fonctionnalités

- Détection d'anomalies par seuil (taux d'erreur > 30% sur fenêtre glissante de 1 minute)
- Archivage automatique vers S3 (format Parquet, écriture toutes les minutes)
- Alerting Slack avec mécanisme de cooldown (5 minutes par service)
- Rapports journaliers automatisés (DAG Airflow)
- Politique de rétention automatisée (DAG Airflow)
- Infrastructure entièrement reproductible via Terraform
- Pipeline CI/CD (lint, validation Terraform, scan de secrets)

## Auteur

Achraf — Projet personnel DevOps/Cloud/Big Data
