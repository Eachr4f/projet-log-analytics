from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

BUCKET_NAME = "log-analytics-achraf-2026"
RETENTION_DAYS = 1  # seuil court pour tester ; passer à 30 en usage réel

default_args = {
    "owner": "achraf",
    "retries": 1,
}

def cleanup_old_logs(**context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="logs-archive/")

    if "Contents" not in response:
        print("Rien à nettoyer.")
        return

    cutoff = datetime.now(response["Contents"][0]["LastModified"].tzinfo) - timedelta(days=RETENTION_DAYS)
    deleted_count = 0
    freed_bytes = 0

    for obj in response["Contents"]:
        if obj["LastModified"] < cutoff:
            s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            deleted_count += 1
            freed_bytes += obj["Size"]

    print(f"Nettoyage terminé: {deleted_count} fichiers supprimés, {freed_bytes / 1024:.2f} Ko libérés.")

with DAG(
    dag_id="dag_nettoyage_retention",
    default_args=default_args,
    description="Supprime les logs archivés au-delà de la période de rétention",
    schedule_interval="0 1 * * *",  # tous les jours à 1h, après l'agrégation
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["log-analytics", "cleanup"],
) as dag:

    task_cleanup = PythonOperator(
        task_id="cleanup_old_logs",
        python_callable=cleanup_old_logs,
    )
