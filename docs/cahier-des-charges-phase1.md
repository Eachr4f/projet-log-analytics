# Cahier des charges — Plateforme d'Analyse de Logs à Grande Échelle

**Projet personnel DevOps / Cloud / Big Data**
**Environnement de développement :** Fedora Linux, 16 Go RAM, i5 11e gen, 512 Go SSD

---

## 1. Contexte et objectifs

### 1.1 Contexte
Les applications modernes (microservices, APIs, sites web) génèrent des volumes massifs de logs. Sans centralisation ni analyse automatisée, il devient impossible de détecter rapidement des incidents, des comportements anormaux ou des menaces de sécurité.

### 1.2 Objectif du projet
Concevoir et déployer une plateforme capable de :
- Centraliser des logs applicatifs en provenance de plusieurs sources
- Traiter ces logs en temps réel (streaming) et en batch
- Détecter automatiquement des anomalies (pics d'erreurs, comportements suspects, latences anormales)
- Visualiser les données via des dashboards
- Alerter automatiquement les équipes en cas d'anomalie
- Automatiser entièrement le déploiement de l'infrastructure (IaC) et son cycle de vie (CI/CD)

### 1.3 Objectif pédagogique / CV
Démontrer une maîtrise de bout en bout : ingestion de données, traitement distribué, orchestration, infrastructure cloud, sécurité, monitoring et automatisation.

---

## 2. Périmètre du projet

### 2.1 Inclus
- Génération de logs simulés (applications factices)
- Pipeline d'ingestion temps réel
- Traitement et détection d'anomalies
- Stockage local (archives) et cloud (S3/GCS)
- Indexation et recherche (Elasticsearch/OpenSearch ou Loki)
- Dashboards de visualisation (Grafana)
- Orchestration batch (Airflow)
- Alerting (email/Slack)
- Infrastructure as Code (Terraform)
- Pipeline CI/CD (GitHub Actions)
- Documentation technique complète

### 2.2 Exclus
- Développement d'une vraie application métier (on simule uniquement les logs)
- Support multi-tenant / multi-clients
- Interface utilisateur web sur-mesure (on s'appuie sur Grafana/Kibana existants)

---

## 3. Architecture générale

### 3.1 Vue d'ensemble du flux de données

```
Sources de logs (apps simulées)
        ↓
   Apache Kafka (ingestion, buffer)
        ↓
   Spark Streaming (traitement temps réel + détection d'anomalies)
        ↓
   ┌────────────┬─────────────────┐
   ↓            ↓                 ↓
Stockage      Indexation      Alerting
(S3/MinIO)   (Elasticsearch/  (Lambda/SNS
 archive       Loki)           ou Slack Webhook)
                ↓
            Grafana / Kibana
            (dashboards)

Orchestration batch (jobs d'agrégation, nettoyage, rapports)
   → Apache Airflow

Infrastructure
   → Terraform (provisioning cloud)
   → GitHub Actions (CI/CD)
   → Docker Compose (services locaux)
```

### 3.2 Répartition local / cloud
| Composant | Emplacement | Justification |
|---|---|---|
| Générateur de logs | Local (Docker) | Pas besoin de coût cloud pour simuler |
| Kafka | Local (Docker) | Contrôle total, pas de frais |
| Spark Streaming | Local (Docker) | Traitement lourd, évite les coûts de calcul cloud |
| Stockage archive | Cloud (S3/GCS) | Démontre la maîtrise du stockage objet cloud |
| Elasticsearch/OpenSearch | Cloud (managé) ou local (Loki) | Selon budget/RAM disponible |
| Grafana | Cloud (Grafana Cloud free tier) | Évite de consommer de la RAM locale |
| Airflow | Local (Docker) ou VM cloud gratuite | Selon avancement |
| Alerting | Cloud (Lambda/SNS ou Webhook Slack) | Service managé, serverless |
| Infrastructure | Terraform (déclenché en local) | Provisionne les ressources cloud |

---

## 4. Spécifications fonctionnelles

### 4.1 Génération et ingestion des logs
- Simuler plusieurs "applications" générant des logs au format structuré (JSON) : timestamp, niveau (INFO/WARN/ERROR), service source, message, code HTTP, latence, IP utilisateur
- Générer un flux continu avec un débit variable (simuler une charge normale et des pics)
- Publier ces logs vers un topic Kafka dédié

### 4.2 Traitement temps réel
- Consommer les logs depuis Kafka
- Parser et nettoyer les données (validation du format, enrichissement — ex: géolocalisation IP)
- Calculer des métriques glissantes (nombre d'erreurs par minute, latence moyenne, taux d'erreur par service)
- Appliquer une logique de détection d'anomalies (voir section 4.4)
- Écrire les résultats vers le stockage cloud et le moteur de recherche

### 4.3 Traitement batch (orchestration)
- Planifier des jobs quotidiens : agrégation des logs de la journée, génération de rapports statistiques, nettoyage des données archivées (rétention)
- Orchestrer ces jobs via Airflow avec gestion des dépendances et des reprises sur erreur

### 4.4 Détection d'anomalies
Définir et implémenter au moins deux approches :
- **Approche par seuils (rule-based)** : ex. taux d'erreur > 5% sur une fenêtre de 1 minute, latence moyenne > seuil défini
- **Approche statistique** : détection d'écarts par rapport à une moyenne mobile (ex. z-score, écart-type)
- (Optionnel, bonus) Approche par modèle ML simple (isolation forest ou clustering) pour détecter des patterns inhabituels

### 4.5 Stockage
- Archivage brut des logs (format Parquet ou JSON compressé) dans un bucket cloud, partitionné par date
- Politique de rétention définie (ex. 30 jours en accès rapide, archivage long terme au-delà)

### 4.6 Recherche et visualisation
- Indexer les logs traités pour permettre une recherche full-text rapide
- Construire au minimum 3 dashboards Grafana :
  1. Vue d'ensemble (volume de logs, répartition par niveau, par service)
  2. Vue anomalies (alertes déclenchées, historique, tendance)
  3. Vue performance (latences, taux d'erreur par service dans le temps)

### 4.7 Alerting
- Déclenchement automatique d'une notification (email ou Slack) lors de la détection d'une anomalie
- Contenu de l'alerte : service concerné, type d'anomalie, valeur mesurée vs seuil, horodatage
- Éviter le spam d'alertes (mécanisme de cooldown / regroupement)

### 4.8 Infrastructure as Code
- Toutes les ressources cloud (bucket de stockage, service de recherche managé, rôles IAM, fonction serverless d'alerting) provisionnées via Terraform
- Code Terraform versionné, modulaire (un module par composant)
- Variables et secrets externalisés (pas de valeurs en dur)

### 4.9 CI/CD
- Pipeline déclenché à chaque push :
  - Validation du code (lint)
  - Tests (si applicable, ex. tests unitaires sur la logique de détection d'anomalies)
  - Build des images Docker
  - Scan de sécurité des images
  - Déploiement automatique (ou manuel avec validation) des changements d'infrastructure Terraform

---

## 5. Spécifications non-fonctionnelles

| Critère | Exigence |
|---|---|
| **Performance** | Le pipeline doit absorber un débit simulé d'au moins 1000 logs/seconde sans perte |
| **Latence de détection** | Une anomalie doit être détectée et alertée en moins de 2 minutes après son apparition |
| **Disponibilité** | Les services locaux doivent pouvoir redémarrer automatiquement en cas de crash (restart policy Docker) |
| **Sécurité** | Aucun identifiant/secret en clair dans le code ; utilisation de variables d'environnement ou d'un gestionnaire de secrets |
| **Coût** | Rester dans les limites des free tiers cloud (0€ ou quasi nul) |
| **Ressources locales** | Consommation RAM maîtrisée (max ~10-12 Go simultanés) pour rester fonctionnel sur la machine de dev |
| **Documentation** | Architecture, choix techniques et procédure de déploiement documentés (README détaillé) |
| **Reproductibilité** | L'ensemble du projet doit pouvoir être redéployé depuis zéro via un script/commande unique |

---

## 6. Contraintes techniques liées à l'environnement

- **RAM limitée (16 Go)** : limiter le nombre de conteneurs actifs simultanément, privilégier des images légères (ex. Bitnami), préférer Loki à Elasticsearch si la RAM devient un facteur bloquant
- **Stockage (512 Go SSD)** : prévoir un nettoyage régulier des images/volumes Docker inutilisés
- **CPU (i5 11e gen)** : éviter de lancer Spark en cluster multi-nœuds complet localement ; privilégier le mode local avec parallélisme limité
- **Cloud** : utiliser exclusivement les offres gratuites (AWS Free Tier, GCP Free Tier, Oracle Cloud Free Tier, Grafana Cloud Free) pour éviter tout coût

---

## 7. Découpage en phases (roadmap)

### Phase 1 — Fondations locales
- Mise en place de Kafka en local (Docker Compose)
- Développement du générateur de logs simulés
- Vérification du flux de bout en bout (génération → Kafka)

### Phase 2 — Traitement temps réel
- Mise en place de Spark Streaming
- Implémentation du parsing, de l'enrichissement et des métriques glissantes
- Implémentation de la logique de détection d'anomalies (seuils simples d'abord)

### Phase 3 — Stockage et recherche
- Provisioning du bucket cloud via Terraform
- Écriture des logs archivés vers le cloud
- Mise en place du moteur de recherche (Loki ou OpenSearch)

### Phase 4 — Visualisation et alerting
- Construction des dashboards Grafana
- Implémentation du mécanisme d'alerting (Lambda/SNS ou webhook)
- Tests de bout en bout avec injection d'anomalies simulées

### Phase 5 — Orchestration batch
- Mise en place d'Airflow
- Développement des DAGs (agrégation, rapports, nettoyage/rétention)

### Phase 6 — Industrialisation
- Écriture complète du code Terraform (modularisation)
- Mise en place du pipeline CI/CD (GitHub Actions)
- Ajout des scans de sécurité (images Docker, secrets)

### Phase 7 — Finalisation
- Rédaction de la documentation complète (architecture, guide de déploiement, captures d'écran des dashboards)
- Tests de charge et validation des exigences non-fonctionnelles
- Préparation d'une démonstration (vidéo ou schéma commenté) pour le portfolio/CV

---

## 8. Livrables attendus

1. Code source versionné sur Git (dépôt public ou privé avec README)
2. Docker Compose pour tous les services locaux
3. Code Terraform pour l'infrastructure cloud
4. DAGs Airflow
5. Dashboards Grafana exportés (JSON)
6. Pipeline CI/CD fonctionnel (fichier de configuration GitHub Actions)
7. Documentation technique (architecture, choix techniques, instructions de déploiement)
8. Support de présentation synthétique (schéma d'architecture + résultats obtenus) pour le CV/portfolio

---

## 9. Critères de succès

- Le pipeline traite un flux de logs simulé sans interruption sur au moins 30 minutes en continu
- Une anomalie injectée manuellement est détectée et une alerte est reçue en moins de 2 minutes
- L'infrastructure cloud peut être détruite et recréée entièrement via `terraform destroy` / `terraform apply`
- Les dashboards affichent des données cohérentes et à jour
- Le déploiement complet (local + cloud) peut être reproduit par une tierce personne en suivant uniquement la documentation

---

## 10. Tableau récapitulatif des outils

| Catégorie | Outil | Rôle dans le projet | Emplacement |
|---|---|---|---|
| **Génération de logs** | Script custom (Python/Faker) | Simuler des logs applicatifs réalistes | Local |
| **Conteneurisation** | Docker | Faire tourner tous les services locaux | Local |
| **Orchestration de conteneurs** | Docker Compose | Définir et lancer la stack multi-services | Local |
| **Ingestion / Message broker** | Apache Kafka | Buffer et distribution des logs en flux continu | Local (Docker) |
| **Interface Kafka** | Kafka UI (ou AKHQ) | Visualiser les topics et messages Kafka | Local (Docker) |
| **Traitement temps réel** | Apache Spark Streaming | Parsing, enrichissement, calcul de métriques, détection d'anomalies | Local (Docker) |
| **Traitement statistique** | PySpark / bibliothèques statistiques (z-score, etc.) | Logique de détection d'anomalies | Local |
| **Orchestration batch** | Apache Airflow | Planification des jobs (agrégation, nettoyage, rapports) | Local (Docker) ou VM cloud |
| **Stockage objet** | AWS S3 / GCP Cloud Storage | Archivage des logs bruts (partitionné par date) | Cloud |
| **Stockage objet (alternative locale)** | MinIO | Substitut S3 léger pour tests locaux avant migration cloud | Local (option) |
| **Format de données** | Parquet / JSON compressé | Format de stockage optimisé pour l'archivage | — |
| **Indexation / recherche** | Elasticsearch / Amazon OpenSearch | Indexation et recherche full-text des logs | Cloud (managé) |
| **Indexation / recherche (alternative légère)** | Grafana Loki | Alternative moins gourmande en RAM qu'Elasticsearch | Local ou Cloud |
| **Visualisation / dashboards** | Grafana (Grafana Cloud free tier) | Dashboards temps réel (volumes, anomalies, performance) | Cloud |
| **Visualisation (alternative)** | Kibana | Visualisation si utilisation d'Elasticsearch | Cloud/Local |
| **Alerting / notifications** | AWS Lambda + SNS | Déclenchement d'alertes serverless | Cloud |
| **Alerting (alternative simple)** | Webhook Slack / Discord | Notifications directes sans infra serverless | Cloud (SaaS) |
| **Infrastructure as Code** | Terraform | Provisionnement automatisé de toutes les ressources cloud | Local (exécution) → Cloud (cibles) |
| **Gestion des secrets** | Variables d'environnement / HashiCorp Vault (optionnel) | Éviter les identifiants en dur dans le code | Local/Cloud |
| **Contrôle de version** | Git + GitHub (ou GitLab) | Versionner tout le code du projet | — |
| **CI/CD** | GitHub Actions (ou GitLab CI) | Pipeline de build, test, scan et déploiement automatisé | Cloud (SaaS) |
| **Scan de sécurité des images** | Trivy | Détection de vulnérabilités dans les images Docker | Local/CI |
| **Détection de secrets** | GitLeaks | Empêcher la fuite de credentials dans le code versionné | CI |
| **Provider cloud principal** | AWS (free tier) | Hébergement S3, Lambda, SNS, IAM | Cloud |
| **Provider cloud alternatif** | GCP ou Oracle Cloud (free tier) | Alternative pour Pub/Sub, VM gratuite (Oracle) | Cloud |
| **Documentation** | Markdown + schémas (diagrams.net / Excalidraw) | Documentation technique et schémas d'architecture | Local |

---

## 11. Compétences démontrées (pour valorisation CV)

- Ingestion et traitement de flux de données à grande échelle (Kafka, Spark Streaming)
- Orchestration de workflows (Airflow)
- Infrastructure as Code (Terraform)
- Conteneurisation et orchestration locale (Docker, Docker Compose)
- Intégration cloud hybride (stockage objet, services managés, serverless)
- Observabilité et monitoring (Grafana, alerting)
- CI/CD et DevSecOps (GitHub Actions, scan de sécurité)
- Détection d'anomalies et bases de data engineering
