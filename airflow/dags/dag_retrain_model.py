from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

BUCKET_NAME = "log-analytics-achraf-2026"
FEATURE_COLUMNS = ["total_logs", "avg_latency_ms", "error_rate"]

default_args = {
    "owner": "achraf",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def retrain_model(**context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="logs-archive/")

    if "Contents" not in response:
        print("Aucune donnée disponible pour le réentraînement.")
        return

    parquet_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
    dfs = []
    for key in parquet_files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        dfs.append(df)

    if not dfs:
        print("Pas de données à traiter.")
        return

    raw_df = pd.concat(dfs, ignore_index=True)
    raw_df["event_time"] = pd.to_datetime(raw_df["timestamp"], unit="s", errors="coerce")
    raw_df = raw_df.dropna(subset=["event_time"])
    raw_df["window"] = raw_df["event_time"].dt.floor("1min")

    features = raw_df.groupby(["window", "service"]).agg(
        total_logs=("service", "count"),
        avg_latency_ms=("latency_ms", "mean"),
        error_count=("level", lambda x: (x == "ERROR").sum())
    ).reset_index()
    features["error_rate"] = features["error_count"] / features["total_logs"]
    features["avg_latency_ms"] = features["avg_latency_ms"].fillna(0)

    X = features[FEATURE_COLUMNS].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X_scaled)

    predictions = model.predict(X_scaled)
    n_anomalies = (predictions == -1).sum()
    print(f"Réentraînement terminé: {n_anomalies}/{len(features)} anomalies détectées sur l'historique.")

    version_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    joblib.dump(model, f"/tmp/isolation_forest_{version_tag}.joblib")
    joblib.dump(scaler, f"/tmp/scaler_{version_tag}.joblib")

    # Upload versionné (historique) + écrasement de la version "latest" utilisée par Spark
    s3.upload_file(f"/tmp/isolation_forest_{version_tag}.joblib", BUCKET_NAME, f"models/isolation_forest_{version_tag}.joblib")
    s3.upload_file(f"/tmp/isolation_forest_{version_tag}.joblib", BUCKET_NAME, "models/isolation_forest_v1.joblib")
    s3.upload_file(f"/tmp/scaler_{version_tag}.joblib", BUCKET_NAME, "models/scaler_v1.joblib")

    print(f"Nouveau modèle déployé: models/isolation_forest_v1.joblib (version {version_tag})")

with DAG(
    dag_id="dag_retrain_model",
    default_args=default_args,
    description="Réentraîne le modèle de détection d'anomalies (Isolation Forest) chaque semaine",
    schedule_interval="0 2 * * 0",  # tous les dimanches à 2h du matin
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["log-analytics", "ml", "retrain"],
) as dag:

    task_retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model,
    )
