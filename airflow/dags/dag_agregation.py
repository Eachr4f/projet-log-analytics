from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io

BUCKET_NAME = "log-analytics-achraf-2026"

default_args = {
    "owner": "achraf",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def aggregate_daily_stats(**context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="logs-archive/")

    if "Contents" not in response:
        print("Aucune donnée trouvée dans le bucket.")
        return

    parquet_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
    print(f"{len(parquet_files)} fichiers Parquet trouvés.")

    dfs = []
    for key in parquet_files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        dfs.append(df)

    if not dfs:
        print("Aucune donnée à agréger.")
        return

    full_df = pd.concat(dfs, ignore_index=True)

    summary = full_df.groupby("service").agg(
        total_logs=("service", "count"),
        avg_latency=("latency_ms", "mean"),
        error_count=("level", lambda x: (x == "ERROR").sum())
    ).reset_index()

    summary["error_rate"] = summary["error_count"] / summary["total_logs"]

    print("=== Résumé journalier ===")
    print(summary.to_string(index=False))

    # Écriture du rapport vers S3
    buffer = io.BytesIO()
    summary.to_parquet(buffer, index=False)
    buffer.seek(0)
    report_key = f"reports/summary-{datetime.now().strftime('%Y-%m-%d')}.parquet"
    s3.put_object(Bucket=BUCKET_NAME, Key=report_key, Body=buffer.getvalue())
    print(f"Rapport écrit: s3://{BUCKET_NAME}/{report_key}")

with DAG(
    dag_id="dag_agregation_journaliere",
    default_args=default_args,
    description="Agrège les statistiques journalières depuis les logs archivés",
    schedule_interval="30 0 * * *",  # tous les jours à 00h30
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["log-analytics", "aggregation"],
) as dag:

    task_aggregate = PythonOperator(
        task_id="aggregate_daily_stats",
        python_callable=aggregate_daily_stats,
    )
