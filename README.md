# 📊 Plateforme d'Analyse de Logs à Grande Échelle

Pipeline complet de collecte, traitement en temps réel, détection d'anomalies (règles + Machine Learning), archivage cloud et visualisation de logs applicatifs, avec orchestration batch, infrastructure Kubernetes et double pipeline CI/CD.

> Projet personnel DevOps / Cloud / Big Data / MLOps — développé sur Fedora Linux (16 Go RAM, i7 11e gen, 512 Go SSD)
> **Statut : Phase 1 (fondations) et Phase 2 (professionnalisation) complètes** — voir les tags [`v1.0-phase1`](../../releases/tag/v1.0-phase1) et [`v2.0-phase2`](../../releases/tag/v2.0-phase2)

---

## 📐 Architecture

<!--
  Pour insérer l'image du schéma d'architecture :
  1. Exporte le diagramme (depuis mermaid.live : bouton "Export" > PNG ou SVG)
  2. Place le fichier exporté dans docs/, ex: docs/architecture.png
  3. La ligne ci-dessous l'affichera automatiquement
-->

![Architecture du projet](docs/architecture.png)

### Vue d'ensemble du flux

```
Nginx (trafic HTTP réel) → Fluent Bit → Kafka (Kubernetes / Strimzi)
                                              ↓
                                    Spark Streaming (local)
                                    ├── Détection par seuil fixe
                                    └── Détection par modèle ML (Isolation Forest)
                                              ↓                    ↓
                                    Stockage S3 (Parquet)   Topic Kafka anomalies
                                              ↓                    ↓
                                    Loki (K8s) → Grafana (K8s)   Alerting Slack

Orchestration batch (Airflow) : agrégation quotidienne + nettoyage/rétention + réentraînement ML hebdomadaire
Infrastructure : Terraform (cloud AWS) + Kubernetes/Kind/Strimzi/Helm (local)
CI/CD : GitHub Actions + Jenkins (déploiement Kubernetes automatisé)
```

Un schéma Mermaid détaillé et éditable est disponible dans [`docs/architecture.mermaid`](docs/architecture.mermaid) — modifiable directement sur [mermaid.live](https://mermaid.live).

### Pourquoi cette répartition local / cloud / Kubernetes

| Composant | Emplacement | Justification |
|---|---|---|
| Nginx + Fluent Bit | Docker Compose (local) | Source de logs, pas de bénéfice à conteneuriser sur K8s pour ce volume |
| Kafka | **Kubernetes** (Strimzi, mode KRaft) | Gestion déclarative des topics — résout la perte de topics rencontrée avec Kafka en simple conteneur |
| Loki + Grafana | **Kubernetes** (Helm charts officiels) | Composants stateless, bénéficient du redémarrage/scaling géré par K8s |
| Spark Streaming | Local (hors cluster) | Évite la complexité du Spark Operator et la surcharge RAM sur un poste de développement à 16 Go |
| Airflow | Docker Compose (hors cluster) | Même arbitrage RAM ; pas de second executor Kubernetes à gérer |
| Modèle ML (Isolation Forest) | Intégré au job Spark | Cohérent avec le traitement temps réel déjà en place |
| Jenkins | Docker (peut cibler le cluster Kind via kubectl) | CI/CD complémentaire à GitHub Actions, avec capacité de déploiement infra |

> Spark et Airflow restent volontairement hors du cluster Kubernetes — un arbitrage documenté pour préserver les performances de développement local, pas une limitation subie.

---

## 🧰 Stack technique

| Domaine | Outil |
|---|---|
| Source de logs | Nginx (trafic HTTP réel) |
| Collecte de logs | Fluent Bit |
| Ingestion de messages | Apache Kafka 4.2.0 (mode KRaft) sur **Kubernetes via Strimzi** |
| Orchestration de conteneurs | Kubernetes (Kind) + Helm |
| Traitement temps réel | Apache Spark Streaming (PySpark 4.2.0) |
| Détection d'anomalies | Règle de seuil fixe **+** modèle Isolation Forest (scikit-learn) |
| Stockage objet | AWS S3 (format Parquet) |
| Indexation / recherche de logs | Grafana Loki (sur Kubernetes) |
| Visualisation | Grafana (sur Kubernetes, 3 dashboards) |
| Alerting | Slack (webhook entrant + cooldown anti-spam) |
| Orchestration batch | Apache Airflow (LocalExecutor, 3 DAGs) |
| Infrastructure as Code | Terraform (provider AWS) |
| CI/CD | GitHub Actions **+** Jenkins (déploiement Kubernetes automatisé) |
| Conteneurisation | Docker, Docker Compose, Kind |
| Langage principal | Python 3.11+ |

---

## 📁 Structure du projet

```
projet-log-analytics/
├── docker-compose.yml          # Kafka UI, Nginx, Fluent Bit, Jenkins
├── nginx/                      # Configuration Nginx (logs JSON)
├── fluent-bit/                 # Configuration Fluent Bit + script de transformation Lua
├── load-generator/             # Script de génération de trafic HTTP
├── processing/                 # Traitement Spark Streaming + scoring ML
│   └── process_logs.py
├── loki-forwarder/             # Pont Kafka → Loki
│   └── forwarder.py
├── alerting/                   # Consumer d'alertes Slack
│   └── alert_consumer.py
├── ml/                         # Entraînement du modèle de détection d'anomalies
│   ├── extract_training_data.py
│   └── train_model.py
├── airflow/                    # Orchestration batch
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── dags/
│       ├── dag_agregation.py
│       ├── dag_nettoyage.py
│       └── dag_retrain_model.py
├── terraform/                  # Infrastructure as Code (cloud AWS)
│   ├── main.tf
│   └── variables.tf
├── k8s/                        # Manifests Kubernetes
│   ├── kind-config.yaml
│   ├── kafka-nodepool.yaml
│   ├── kafka-cluster.yaml
│   ├── topic-logs-raw.yaml
│   ├── topic-logs-anomalies.yaml
│   └── helm-values/
│       ├── loki-values.yaml
│       └── grafana-values.yaml
├── .github/workflows/
│   └── ci.yml                  # Pipeline GitHub Actions
├── Jenkinsfile                 # Pipeline Jenkins (déploiement K8s)
├── docs/
│   ├── architecture.png
│   ├── architecture.mermaid
│   └── cahier-des-charges-phase2.md
└── README.md
```

---

## ✅ Prérequis

- Linux (testé sur Fedora) avec Docker et Docker Compose
- Python 3.11+
- Java 17 (recommandé via [SDKMAN](https://sdkman.io/), en coexistence avec une version système plus récente si besoin)
- [Kind](https://kind.sigs.k8s.io/), `kubectl`, [Helm](https://helm.sh/)
- Un compte AWS avec un utilisateur IAM disposant d'un accès S3 limité (principe du moindre privilège)
- Un compte Slack avec un webhook entrant configuré
- Terraform CLI

---

## 🚀 Installation et lancement

### 1. Cloner le dépôt

```bash
git clone https://github.com/Eachr4f/projet-log-analytics.git
cd projet-log-analytics
```

### 2. Créer le cluster Kubernetes local

```bash
kind create cluster --name log-analytics --config k8s/kind-config.yaml
```

### 3. Déployer Kafka via Strimzi

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Attendre que l'opérateur soit Running avant de continuer
kubectl get pods -n kafka --watch

kubectl apply -f k8s/kafka-nodepool.yaml
kubectl apply -f k8s/kafka-cluster.yaml
kubectl apply -f k8s/topic-logs-raw.yaml
kubectl apply -f k8s/topic-logs-anomalies.yaml
```

> ⚠️ Le fichier `k8s/kafka-cluster.yaml` référence une adresse IP (`advertisedHost`) pour l'accès externe au broker — adapte-la à l'IP réseau locale de ta machine avant d'appliquer. Cette IP doit être mise à jour à chaque changement de réseau (voir section Dépannage).

### 4. Déployer Loki et Grafana via Helm

```bash
kubectl create namespace observability
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

helm install loki grafana/loki -n observability -f k8s/helm-values/loki-values.yaml
helm install grafana grafana/grafana -n observability -f k8s/helm-values/grafana-values.yaml
```

Accès (via port-forward, dans des terminaux dédiés) :
```bash
kubectl port-forward -n observability svc/grafana 3000:80
kubectl port-forward -n observability svc/loki 3100:3100
```

### 5. Lancer l'infrastructure Docker locale (Nginx, Fluent Bit, Kafka UI)

```bash
docker compose up -d
```

### 6. Configurer les accès AWS

```bash
aws configure
```

### 7. Provisionner l'infrastructure cloud avec Terraform

```bash
cd terraform
terraform init
terraform apply
cd ..
```

### 8. Générer du trafic

```bash
cd load-generator
chmod +x load_test.sh
./load_test.sh
```

### 9. Entraîner le modèle de détection d'anomalies (première fois)

```bash
cd ml
python3 -m venv venv
source venv/bin/activate
pip install scikit-learn pandas pyarrow boto3 joblib

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)

python3 extract_training_data.py
python3 train_model.py
```

### 10. Lancer le traitement Spark (détection seuil + ML)

```bash
cd processing
python3 -m venv venv
source venv/bin/activate
pip install pyspark scikit-learn joblib boto3 pandas pyarrow

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0 \
  process_logs.py
```

### 11. Lancer le forwarder vers Loki

```bash
cd loki-forwarder
python3 -m venv venv
source venv/bin/activate
pip install confluent-kafka requests
python3 forwarder.py
```

### 12. Lancer le consumer d'alertes Slack

```bash
cd alerting
python3 -m venv venv
source venv/bin/activate
pip install confluent-kafka requests

export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/TON/WEBHOOK/URL"
python3 alert_consumer.py
```

### 13. Lancer Airflow

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

### 14. Lancer Jenkins

```bash
docker compose up -d jenkins
docker exec -it jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Configure le job pipeline en pointant vers ce dépôt (voir [`Jenkinsfile`](Jenkinsfile)), avec un credential GitHub (scope `repo` + `workflow`) et le `kubeconfig` du cluster Kind copié dans le conteneur.

### 15. Accéder aux interfaces

| Service | URL | Identifiants |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 (port-forward) | admin / admin |
| Airflow | http://localhost:8081 | airflow / airflow |
| Jenkins | http://localhost:8082 | (compte créé à l'installation) |

---

## 🔍 Fonctionnalités

### Traitement et détection
- **Traitement temps réel** : calcul de métriques (volume, latence moyenne, taux d'erreur) par service sur des fenêtres glissantes de 1 minute
- **Détection d'anomalies à deux niveaux** :
  - Règle de seuil fixe (taux d'erreur > 30 %) — simple, explicable, pour les cas évidents
  - Modèle Machine Learning (Isolation Forest) — détecte des anomalies subtiles (ex : volume de trafic anormalement bas) invisibles à une règle de seuil
- **Réentraînement automatique** du modèle chaque semaine via un DAG Airflow, à partir des données les plus récentes

### Stockage et observabilité
- **Archivage cloud** : écriture continue des logs traités vers S3 au format Parquet, partitionné par date
- **Observabilité** : 3 dashboards Grafana (vue d'ensemble, anomalies en direct, latence P95 par service)
- **Alerting intelligent** : notification Slack automatique avec cooldown de 5 minutes par service pour éviter le spam d'alertes

### Orchestration et infrastructure
- **Orchestration batch** (Airflow) : agrégation quotidienne, nettoyage/rétention, réentraînement ML hebdomadaire
- **Infrastructure cloud reproductible** : bucket S3 et règles de sécurité gérés par Terraform
- **Infrastructure Kubernetes déclarative** : Kafka (Strimzi) et observabilité (Helm) définis en YAML versionné, résilients aux redémarrages de pods
- **Double CI/CD** : GitHub Actions (lint, validation Terraform, scan de secrets) et Jenkins (mêmes contrôles + déploiement Kubernetes automatisé)

---

## 🔐 Sécurité

- Aucun secret (clé AWS, webhook Slack) n'est stocké en dur dans le code — tout passe par des variables d'environnement
- Utilisateur IAM dédié avec permissions limitées à S3 (principe du moindre privilège)
- Accès public bloqué sur le bucket S3 (`aws_s3_bucket_public_access_block`)
- Scan automatique de secrets dans les deux pipelines CI/CD
- Historique Git nettoyé (BFG Repo-Cleaner) suite à une exposition accidentelle de webhook, avec révocation immédiate du secret concerné

---

## 🛠️ Dépannage — problèmes fréquents

| Symptôme | Cause | Solution |
|---|---|---|
| Topics Kafka absents après redémarrage | Kafka en simple conteneur ne persiste pas les topics | Résolu par la migration vers Strimzi (Kubernetes) — les topics sont redéfinis automatiquement |
| `kubectl` renvoie une page de login Jenkins | `$HOME` incorrect dans un conteneur, mauvais kubeconfig utilisé | Spécifier `--kubeconfig` explicitement ou définir `KUBECONFIG` en variable d'environnement |
| Erreur de certificat TLS avec Kind | Le certificat de l'API ne couvre pas l'IP réseau locale | `kubectl config set-cluster <nom-cluster> --insecure-skip-tls-verify=true` |
| Connexion Kafka refusée depuis Fluent Bit / Spark | IP réseau locale changée (nouveau Wi-Fi, redémarrage routeur) | Mettre à jour `advertisedHost` dans `k8s/kafka-cluster.yaml`, la config Fluent Bit, et le kubeconfig |
| Batches Spark vides malgré un flux actif | Checkpoint Spark périmé après un changement d'infrastructure Kafka | `rm -rf /tmp/checkpoints` puis relancer le job |
| Grafana "No logs found" alors que les données existent | Cache navigateur ou ancienne instance Grafana encore active sur le même port | Vérifier qu'aucun ancien conteneur Docker Compose n'occupe le port, tester en navigation privée |

---

## 🧠 Compétences démontrées

### Data Engineering
- Ingestion et traitement de flux de données à grande échelle (Kafka, Spark Streaming)
- Calcul de métriques sur fenêtres glissantes et détection d'anomalies temps réel
- Archivage optimisé (format Parquet, partitionnement par date)

### Cloud & Infrastructure
- Infrastructure as Code (Terraform : ressources cloud, gestion d'état, import de ressources existantes)
- Intégration cloud hybride (traitement local, stockage et alerting managés)

### Kubernetes & Conteneurisation
- Déploiement et gestion d'un cluster Kubernetes local (Kind)
- Opérateurs Kubernetes (Strimzi pour Kafka en mode KRaft, gestion déclarative des topics)
- Packaging et déploiement via Helm (Loki, Grafana)
- Résolution de problèmes réseau Kubernetes (NodePort, advertised listeners, certificats TLS, résolution DNS interne au cluster)

### CI/CD & DevSecOps
- Pipelines CI/CD multi-outils (GitHub Actions et Jenkins) avec lint, validation infra et scan de secrets
- Déploiement automatisé d'infrastructure Kubernetes depuis un pipeline CI/CD

### MLOps
- Extraction et préparation de données d'entraînement depuis un data lake S3
- Entraînement, validation et intégration d'un modèle de détection d'anomalies non supervisé (Isolation Forest)
- Réentraînement automatisé et versionné via orchestrateur (Airflow)

### Observabilité
- Dashboards et alerting (Grafana, Loki, Slack avec anti-spam)

### Résolution de problèmes (transférable en entretien)
- Debug de compatibilité de versions en cascade (Kafka/Hadoop/Spark, Strimzi CRD versions, Zookeeper→KRaft)
- Debug réseau multi-conteneurs (Docker Compose ↔ Kubernetes ↔ Jenkins)
- Gestion d'incident de sécurité (exposition de secret, révocation, nettoyage d'historique Git)
- Arbitrage architecture sous contrainte de ressources (documentation des choix, pas seulement leur mise en œuvre)

---

## 📚 Documentation complémentaire

- [Cahier des charges — Phase 1](docs/cahier-des-charges-phase1.md)
- [Cahier des charges — Phase 2](docs/cahier-des-charges-phase2.md)
- [Schéma d'architecture source (Mermaid)](docs/architecture.mermaid)

---

## 📄 Licence

Projet personnel à but éducatif et de démonstration.

## 👤 Auteur

**Achraf (Eachr4f)** — Projet DevOps / Cloud / Big Data / MLOps
