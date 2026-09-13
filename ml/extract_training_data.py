import boto3
import pandas as pd
import io

BUCKET_NAME = "log-analytics-achraf-2026"
PREFIX = "logs-archive/"

def load_all_logs():
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

    if "Contents" not in response:
        print("Aucune donnée trouvée.")
        return pd.DataFrame()

    parquet_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
    print(f"{len(parquet_files)} fichiers Parquet trouvés.")

    dfs = []
    for key in parquet_files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

def build_features(df):
    """Agrège les logs bruts en métriques par fenêtre de 1 minute et par service,
    exactement comme le fait Spark Streaming."""
    df["event_time"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    df = df.dropna(subset=["event_time"])
    df["window"] = df["event_time"].dt.floor("1min")

    features = df.groupby(["window", "service"]).agg(
        total_logs=("service", "count"),
        avg_latency_ms=("latency_ms", "mean"),
        error_count=("level", lambda x: (x == "ERROR").sum())
    ).reset_index()

    features["error_rate"] = features["error_count"] / features["total_logs"]
    features["avg_latency_ms"] = features["avg_latency_ms"].fillna(0)

    return features

if __name__ == "__main__":
    raw_df = load_all_logs()
    if raw_df.empty:
        print("Pas de données à traiter.")
    else:
        features = build_features(raw_df)
        print(f"{len(features)} fenêtres de métriques construites.")
        print(features.describe())
        features.to_parquet("training_features.parquet", index=False)
        print("Sauvegardé: training_features.parquet")
