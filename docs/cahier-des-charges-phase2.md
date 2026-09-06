# Cahier des charges — Phase 2 : Professionnalisation de la Plateforme d'Analyse de Logs

**Projet :** Extension de la plateforme d'analyse de logs (Phase 1 déjà livrée)
**Environnement de développement :** Fedora Linux, 16 Go RAM, i5 11e gen, 512 Go SSD
**Périmètre :** Source de logs réelle, migration Kubernetes, CI/CD Jenkins, détection d'anomalies par Machine Learning

---

## 1. Contexte et objectifs

### 1.1 Contexte
La Phase 1 du projet a permis de livrer un pipeline complet fonctionnel (Kafka, Spark Streaming, S3, Loki/Grafana, alerting Slack, Airflow, Terraform, GitHub Actions) tournant sur Docker Compose, avec un générateur de logs simulés et une détection d'anomalies par seuil fixe.

### 1.2 Objectif de la Phase 2
Faire évoluer cette plateforme vers une architecture plus proche d'un environnement de production réel, en :
- Remplaçant la génération de logs simulés par une collecte de logs applicatifs réels
- Migrant les composants stateless critiques vers Kubernetes pour la scalabilité et la résilience
- Ajoutant un second pipeline CI/CD (Jenkins) capable de déployer sur l'infrastructure Kubernetes
- Remplaçant la détection d'anomalies par seuil fixe par un modèle de Machine Learning non supervisé

### 1.3 Objectif pédagogique / CV
Démontrer une capacité à faire évoluer une architecture existante sans tout reconstruire, à arbitrer entre complexité et contraintes de ressources, et à intégrer des compétences avancées (Kubernetes, MLOps) dans un système déjà en place.

---

## 2. Périmètre de la Phase 2

### 2.1 Inclus
- Collecte de logs depuis une vraie application (Nginx) via un agent dédié (Fluent Bit)
- Migration de Kafka vers Kubernetes via l'opérateur Strimzi (gestion déclarative des topics)
- Migration de Loki et Grafana vers Kubernetes via leurs Helm charts officiels
- Déploiement de Jenkins avec un pipeline CI/CD capable de déployer sur le cluster Kubernetes
- Entraînement et intégration d'un modèle de détection d'anomalies (Isolation Forest)
- Réentraînement périodique du modèle via un DAG Airflow dédié
- Documentation des choix d'architecture (composants gardés hors Kubernetes et pourquoi)

### 2.2 Exclus (décision d'architecture assumée)
- Migration de Spark Streaming vers Kubernetes (reste en local — voir section 6, arbitrage RAM)
- Migration d'Airflow vers Kubernetes (reste en Docker Compose — voir section 6)
- Déploiement sur un cluster Kubernetes managé cloud (EKS/GKE) — reste en local (Kind)
- Mise en place de HashiCorp Vault ou Schema Registry (hors périmètre, mentionné comme axe futur)
- Génération de trafic à très grande échelle (le volume reste adapté à un usage de démonstration locale)

---

## 3. Architecture cible

### 3.1 Vue d'ensemble

```
[Conteneur Nginx] → génère de vraies requêtes HTTP
        ↓ (écrit access.log)
[Fluent Bit] → lit, parse, transforme en JSON structuré
        ↓
┌─────────────────────────── Cluster Kubernetes (Kind) ───────────────────────────┐
│                                                                                    │
│  [Namespace: kafka]                                                               │
│    Strimzi Operator → Kafka (topics déclaratifs logs-raw / logs-anomalies)       │
│                                                                                    │
│  [Namespace: observability]                                                      │
│    Loki (Helm chart) + Grafana (Helm chart)                                      │
│                                                                                    │
└────────────────────────────────────────────────────────────────────────────────┘
        ↓ (Spark reste hors cluster, connexion via NodePort/port-forward)
[Spark Streaming - local] → traitement + application du modèle ML
        ↓                              ↓
[Stockage S3]              [Modèle Isolation Forest (scikit-learn)]
        ↓                              ↓ (scoring anomalies)
                            [Topic logs-anomalies] → Alerting Slack
        ↓
[Airflow - Docker Compose, hors cluster]
   ├── DAG Agrégation (existant)
   ├── DAG Nettoyage (existant)
   └── DAG Réentraînement du modèle ML (nouveau)

[Jenkins - conteneur Docker]
   → Pipeline : lint, tests, build, scan sécurité, déploiement sur le cluster Kind (kubectl apply)
```

### 3.2 Répartition des composants et justification

| Composant | Emplacement | Justification |
|---|---|---|
| Nginx + Fluent Bit | Docker Compose local | Source de logs, pas de bénéfice à conteneuriser sur K8s pour ce volume |
| Kafka (Strimzi) | Kubernetes (Kind) | Résout la persistance des topics, démontre la gestion déclarative |
| Loki + Grafana | Kubernetes (Kind), Helm | Composants stateless, bénéficient de la gestion K8s (redémarrage, scaling) |
| Spark Streaming | Local (hors K8s) | Évite la complexité du Spark Operator et la surcharge RAM ; décision documentée |
| Airflow | Docker Compose (hors K8s) | Évite un deuxième cluster/executor K8s ; RAM limitée à 16 Go |
| Modèle ML (Isolation Forest) | Intégré dans le job Spark | Cohérent avec le traitement temps réel déjà en place |
| Jenkins | Conteneur Docker (peut cibler le cluster Kind) | CI/CD complémentaire à GitHub Actions, démontre un second outil standard entreprise |

---

## 4. Spécifications fonctionnelles

### 4.1 Source de logs réelle (Nginx + Fluent Bit)

- Déployer un conteneur Nginx servant du contenu simple (page statique ou proxy vers une API factice)
- Générer un trafic réaliste et variable (requêtes normales + pics + erreurs simulées via des routes 404/500) à l'aide d'un script de charge
- Configurer Fluent Bit pour :
  - Lire le fichier `access.log` de Nginx
  - Parser le format de log Nginx (regex ou format JSON si Nginx est configuré en log JSON)
  - Enrichir/transformer les champs pour correspondre au schéma attendu par Spark (`service`, `level`, `http_code`, `latency_ms`, `ip`, `timestamp`)
  - Publier vers le topic Kafka `logs-raw`, sans rupture de compatibilité avec le pipeline existant

### 4.2 Migration Kubernetes (Kafka via Strimzi)

- Installer un cluster Kubernetes local (Kind)
- Déployer l'opérateur Strimzi dans un namespace dédié (`kafka`)
- Définir le cluster Kafka comme ressource déclarative (`Kafka` custom resource)
- Définir les topics `logs-raw` et `logs-anomalies` comme ressources déclaratives (`KafkaTopic`), versionnées sur Git
- Vérifier que la persistance des topics est garantie entre les redémarrages du cluster (résolution du problème identifié en Phase 1)
- Exposer Kafka de façon à ce que Spark (resté en local) puisse s'y connecter (NodePort ou port-forward documenté)

### 4.3 Migration Kubernetes (Loki + Grafana)

- Déployer Loki et Grafana via leurs Helm charts officiels, dans un namespace `observability`
- Reconfigurer le forwarder (ou Fluent Bit directement) pour pointer vers le nouveau endpoint Loki
- Recréer les 3 dashboards existants (vue d'ensemble, anomalies, performance) sur la nouvelle instance Grafana
- Documenter l'accès aux interfaces (port-forward ou Ingress local)

### 4.4 Pipeline CI/CD Jenkins

- Déployer Jenkins en conteneur Docker
- Créer un `Jenkinsfile` reproduisant au minimum :
  - Lint du code Python
  - Validation Terraform (`terraform validate`)
  - Scan de secrets (GitLeaks ou équivalent)
- Ajouter une étape supplémentaire absente de GitHub Actions : déploiement automatique des manifests Strimzi/Kafka sur le cluster Kind (`kubectl apply -f`)
- Configurer le déclenchement automatique via webhook GitHub sur push vers `main`

### 4.5 Détection d'anomalies par Machine Learning

- Constituer un jeu de données d'entraînement à partir des métriques historiques déjà archivées dans S3 (Phase 1), représentant un comportement "normal"
- Entraîner un modèle non supervisé (Isolation Forest, scikit-learn) sur ces métriques (volume de logs, latence moyenne, taux d'erreur, par service et par fenêtre de temps)
- Sauvegarder le modèle entraîné dans un format réutilisable (`joblib`), versionné dans S3 (bucket ou préfixe dédié `models/`)
- Intégrer le modèle dans le job Spark Streaming : à chaque fenêtre calculée, scorer les métriques avec le modèle chargé et déclencher une anomalie si le score dépasse un seuil de confiance
- Conserver la détection par seuil fixe existante en parallèle (défense en profondeur, deux mécanismes complémentaires)
- Créer un DAG Airflow dédié au réentraînement périodique du modèle (hebdomadaire), à partir des données les plus récentes

---

## 5. Spécifications non-fonctionnelles

| Critère | Exigence |
|---|---|
| **Compatibilité ascendante** | Le format des messages dans `logs-raw` et `logs-anomalies` ne doit pas changer, pour ne pas casser les composants existants (Spark, dashboards) |
| **Consommation RAM** | Le cluster Kind (Kafka + Loki + Grafana) ne doit pas dépasser ~6-8 Go cumulés, pour laisser de la marge à Spark, Airflow et Jenkins tournant en parallèle |
| **Reproductibilité** | L'ensemble des manifests Kubernetes (Strimzi, Helm values) doit être versionné et permettre une recréation complète du cluster depuis zéro |
| **Non-régression** | Les 3 dashboards Grafana, le mécanisme d'alerting Slack, et les 2 DAGs Airflow existants doivent continuer à fonctionner après migration |
| **Qualité du modèle ML** | Le modèle doit être validé sur un jeu de données de test avant intégration (taux de faux positifs raisonnable, documenté) |
| **Documentation des arbitrages** | Chaque décision de garder un composant hors Kubernetes (Spark, Airflow) doit être justifiée explicitement dans la documentation |

---

## 6. Contraintes techniques et arbitrages assumés

- **RAM limitée (16 Go)** : Spark Streaming et Airflow restent hors du cluster Kubernetes pour éviter une surcharge mémoire liée à l'exécution simultanée d'un cluster K8s complet (Kind + Strimzi + Helm charts) en plus des processus déjà lourds (Spark, JVM Kafka)
- **Simplicité de connexion** : Spark (hors cluster) doit pouvoir atteindre Kafka (dans le cluster) — nécessite une exposition réseau simple (NodePort) plutôt qu'une architecture réseau complexe (Ingress, service mesh), hors périmètre pour cette phase
- **Un seul cluster local** : pas de cluster de production distant (EKS/GKE) dans cette phase — Kind reste suffisant pour démontrer la maîtrise des concepts Kubernetes

---

## 7. Découpage en phases (roadmap)

### Phase 2.1 — Source de logs réelle (≈1 jour)
- Déploiement Nginx + génération de trafic
- Configuration Fluent Bit (parsing + transformation + publication Kafka)
- Validation de bout en bout avec le pipeline Spark existant (aucune régression)

### Phase 2.2 — Migration Kubernetes (≈2-3 jours)
- Installation Kind + kubectl + Helm
- Déploiement Strimzi + définition déclarative de Kafka et des topics
- Déploiement Loki + Grafana via Helm
- Reconnexion de Fluent Bit et Spark au nouveau Kafka
- Recréation des dashboards Grafana
- Validation de la persistance des topics après redémarrage du cluster

### Phase 2.3 — CI/CD Jenkins (≈1 jour)
- Déploiement Jenkins
- Écriture du Jenkinsfile (lint, validation Terraform, scan secrets, déploiement K8s)
- Configuration du webhook GitHub
- Test de bout en bout (push → build → déploiement automatique)

### Phase 2.4 — Détection d'anomalies ML (≈1-2 jours)
- Extraction et préparation des données d'entraînement depuis S3
- Entraînement et validation du modèle Isolation Forest
- Intégration du modèle dans le job Spark Streaming
- Création du DAG Airflow de réentraînement périodique
- Test comparatif : détection par seuil vs détection par modèle sur des anomalies injectées

### Phase 2.5 — Finalisation et documentation
- Mise à jour complète du README (nouvelle architecture, nouveaux prérequis, instructions de déploiement K8s)
- Schéma d'architecture mis à jour
- Rédaction des arbitrages techniques (section 6) dans la documentation finale
- Démonstration de bout en bout sur l'ensemble du pipeline mis à jour

---

## 8. Livrables attendus

1. Code Fluent Bit (fichier de configuration `fluent-bit.conf`) et configuration Nginx
2. Manifests Kubernetes (Strimzi `Kafka`, `KafkaTopic`, values Helm pour Loki/Grafana)
3. `Jenkinsfile` versionné
4. Script d'entraînement du modèle ML (`train_model.py`) et modèle sauvegardé
5. Job Spark mis à jour intégrant le scoring du modèle
6. DAG Airflow de réentraînement (`dag_retrain_model.py`)
7. README mis à jour avec la nouvelle architecture complète
8. Schéma d'architecture Phase 2 (diagramme mis à jour)

---

## 9. Critères de succès

- Le pipeline fonctionne de bout en bout : Nginx → Fluent Bit → Kafka (K8s) → Spark (local, avec ML) → S3/Loki/Grafana → Alerting
- Les topics Kafka survivent à un redémarrage complet du cluster Kind (`kind delete cluster` + recréation via manifests versionnés)
- Le pipeline Jenkins déploie automatiquement une mise à jour des topics Kafka sur simple push Git
- Le modèle ML détecte au moins les mêmes anomalies que le seuil fixe sur un jeu de test, avec un taux de faux positifs documenté et acceptable
- Le DAG de réentraînement s'exécute sans erreur et produit un nouveau modèle utilisable
- L'ensemble de l'architecture (Phase 1 + Phase 2) peut être redéployé depuis zéro par une tierce personne suivant uniquement la documentation

---

## 10. Tableau récapitulatif des nouveaux outils (Phase 2)

| Catégorie | Outil | Rôle | Emplacement |
|---|---|---|---|
| Application source | Nginx | Génère de vrais logs d'accès HTTP | Local (Docker) |
| Génération de charge | wrk / hey / script curl | Simuler du trafic réaliste vers Nginx | Local |
| Collecte de logs | Fluent Bit | Lecture, parsing et transformation des logs Nginx | Local (Docker) |
| Orchestration de conteneurs | Kind (Kubernetes in Docker) | Cluster Kubernetes local léger | Local |
| CLI Kubernetes | kubectl | Interagir avec le cluster | Local |
| Gestion de packages K8s | Helm | Déployer Loki/Grafana via charts officiels | Local |
| Opérateur Kafka | Strimzi | Gestion déclarative de Kafka et des topics sur K8s | Kubernetes (Kind) |
| CI/CD complémentaire | Jenkins | Pipeline alternatif, déploiement K8s automatisé | Local (Docker) |
| Bibliothèque ML | scikit-learn (Isolation Forest) | Détection d'anomalies non supervisée | Intégré au job Spark |
| Sérialisation de modèle | joblib | Sauvegarde/chargement du modèle entraîné | S3 + local |
| Orchestration ML | Airflow (DAG dédié) | Réentraînement périodique du modèle | Docker Compose (existant) |

---

## 11. Compétences démontrées (pour valorisation CV)

- Collecte de logs applicatifs réels avec un agent standard de l'industrie (Fluent Bit)
- Orchestration de conteneurs avec Kubernetes (Kind), gestion déclarative via opérateurs (Strimzi)
- Packaging et déploiement d'applications avec Helm
- CI/CD multi-outils (GitHub Actions et Jenkins) avec déploiement automatisé sur Kubernetes
- Introduction au MLOps : entraînement, intégration et réentraînement automatisé d'un modèle de détection d'anomalies non supervisé
- Capacité à arbitrer une architecture selon des contraintes réelles de ressources (documentation des choix techniques, pas seulement leur mise en œuvre)
- Maintien de la compatibilité ascendante lors d'une migration d'infrastructure progressive (pas de réécriture complète, évolution incrémentale)
