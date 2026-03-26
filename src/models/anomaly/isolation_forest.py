"""
Isolation Forest for Anomaly Detection
Detects unusual contracts based on feature deviations.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pathlib import Path


# Features to use for anomaly detection
FEATURE_COLS = [
    "bid_ratio",
    "vendor_age_days", 
    "single_bidder",
    "type_risk_score",
    "has_modifications",
    "is_large_contract",
    "is_rounded",
    "title_length",
    "suspicious_keyword_count",
    "is_end_of_year",
    "is_monday",
]


class CorruptionDetector:
    """Detect anomalous contracts using Isolation Forest."""
    
    def __init__(self, contamination: float = 0.1):
        """
        Args:
            contamination: Expected proportion of anomalous contracts (0.0-1.0)
        """
        self.contamination = contamination
        self.scaler = StandardScaler()
        self.model = None
        self.is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> "CorruptionDetector":
        """Train the anomaly detector on contract features."""
        
        # Prepare features
        X = df[FEATURE_COLS].copy()
        X = X.fillna(0)  # Handle missing values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train Isolation Forest
        self.model = IsolationForest(
            n_estimators=100,
            contamination=self.contamination,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_scaled)
        
        self.is_fitted = True
        print(f"✅ Trained Isolation Forest on {len(df)} contracts")
        return self
    
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Predict anomaly scores for contracts."""
        
        if not self.is_fitted:
            raise ValueError("Model not fitted. Call fit() first.")
        
        X = df[FEATURE_COLS].copy()
        X = X.fillna(0)
        X_scaled = self.scaler.transform(X)
        
        # Get predictions and scores
        df = df.copy()
        df["anomaly_label"] = self.model.predict(X_scaled)  # -1 = anomaly, 1 = normal
        df["anomaly_score"] = self.model.decision_function(X_scaled)  # Lower = more anomalous
        
        # Normalize score to 0-100 (higher = more suspicious)
        min_score = df["anomaly_score"].min()
        max_score = df["anomaly_score"].max()
        df["suspicion_score"] = 100 * (max_score - df["anomaly_score"]) / (max_score - min_score + 1e-10)
        
        return df
    
    def get_top_suspicious(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Return the N most suspicious contracts."""
        return df.nlargest(n, "suspicion_score")
    
    def summary(self, df: pd.DataFrame) -> dict:
        """Generate summary statistics."""
        anomalies = df[df["anomaly_label"] == -1]
        return {
            "total_contracts": len(df),
            "anomalies_detected": len(anomalies),
            "anomaly_rate": len(anomalies) / len(df) * 100,
            "avg_suspicion_score": df["suspicion_score"].mean(),
            "max_suspicion_score": df["suspicion_score"].max(),
            "high_risk_contracts": len(df[df["suspicion_score"] > 70]),
        }


if __name__ == "__main__":
    # Quick test with mock data
    from scraper_secop import SecopScraper
    from features.engineering import engineer_all_features
    
    # Load and process data
    scraper = SecopScraper()
    contracts = scraper.fetch_contracts()
    contracts = engineer_all_features(contracts)
    
    # Train and predict
    detector = CorruptionDetector(contamination=0.2)
    detector.fit(contracts)
    results = detector.predict(contracts)
    
    # Show top suspicious
    print("\n🚨 Top 5 Most Suspicious Contracts:")
    print(detector.get_top_suspicious(results, n=5)[[
        "contract_id", "title", "vendor", "suspicion_score"
    ]])
    
    # Summary
    print("\n📊 Summary:")
    for k, v in detector.summary(results).items():
        print(f"  {k}: {v}")