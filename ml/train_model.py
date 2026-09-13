import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import boto3
import io

BUCKET_NAME = "log-analytics-achraf-2026"
MODEL_KEY = "models/isolation_forest_v1.joblib"
SCALER_KEY = "models/scaler_v1.joblib"

FEATURE_COLUMNS = ["total_logs", "avg_latency_ms", "error_rate"]

def train():
    df = pd.read_parquet("training_features.parquet")
    print(f"Entraînement sur {len(df)} échantillons.")

    X = df[FEATURE_COLUMNS].fillna(0)

    # Normalisation : indispensable car les échelles diffèrent beaucoup
    # (total_logs peut être ~50, error_rate est entre 0 et 1, latence en ms)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # contamination=0.05 : on suppose qu'environ 5% des fenêtres historiques
    # représentent un comportement anormal
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42
    )
    model.fit(X_scaled)

    # Validation rapide : score sur les données d'entraînement elles-mêmes
    predictions = model.predict(X_scaled)
    n_anomalies = (predictions == -1).sum()
    print(f"Anomalies détectées sur le jeu d'entraînement: {n_anomalies}/{len(df)} ({100*n_anomalies/len(df):.1f}%)")

    # Sauvegarde locale
    joblib.dump(model, "isolation_forest_v1.joblib")
    joblib.dump(scaler, "scaler_v1.joblib")
    print("Modèle et scaler sauvegardés localement.")

    # Upload vers S3
    s3 = boto3.client("s3")
    s3.upload_file("isolation_forest_v1.joblib", BUCKET_NAME, MODEL_KEY)
    s3.upload_file("scaler_v1.joblib", BUCKET_NAME, SCALER_KEY)
    print(f"Modèle uploadé: s3://{BUCKET_NAME}/{MODEL_KEY}")

if __name__ == "__main__":
    train()
