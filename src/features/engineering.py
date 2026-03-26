"""
Feature Engineering for Corruption Detection
Extracts features from raw contract data for ML models.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


def calculate_contract_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate features that help identify suspicious contracts.
    """
    df = df.copy()
    
    # 1. Bid ratio to ceiling (0.9-1.0 is suspicious)
    df["bid_ratio"] = df["contract_value"] / df["ceiling_value"]
    
    # 2. Days since vendor creation (new vendors = higher risk)
    df["vendor_age_days"] = (
        pd.to_datetime(df["award_date"]) - pd.to_datetime(df["vendor_created"])
    ).dt.days
    
    # 3. Single bidder flag
    df["single_bidder"] = (df["num_bidders"] == 1).astype(int)
    
    # 4. Contract type risk score (direct contracts are riskier)
    type_risk = {
        "contratacion_directa": 1.0,
        "licitacion_publica": 0.3,
        "seleccion_abreviada": 0.5,
        "minima_cuantia": 0.7,
    }
    df["type_risk_score"] = df["contract_type"].map(type_risk).fillna(0.5)
    
    # 5. Modification flag
    df["has_modifications"] = (df["modifications"] > 0).astype(int)
    
    # 6. Contract size category
    df["is_large_contract"] = (df["contract_value"] > 100_000_000).astype(int)
    
    # 7. Round number detection (suspicious round amounts)
    df["is_rounded"] = (df["contract_value"] % 10_000_000 == 0).astype(int)
    
    # 8. Composite risk score
    df["risk_score"] = (
        (df["bid_ratio"] > 0.95).astype(int) * 0.2 +
        (df["vendor_age_days"] < 365).astype(int) * 0.2 +
        df["single_bidder"] * 0.25 +
        df["type_risk_score"] * 0.15 +
        df["has_modifications"] * 0.1 +
        df["is_rounded"] * 0.1
    )
    
    return df


def extract_text_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract features from contract text (title/description).
    """
    df = df.copy()
    
    # Contract title length
    df["title_length"] = df["title"].str.len()
    
    # Common suspicious keywords
    suspicious_keywords = [
        "consultoría", "asesoría", "estudios", "diagnóstico",
        "apoyo", "assistência", "gerencia", "interventoría"
    ]
    
    for kw in suspicious_keywords:
        df[f"has_{kw}"] = df["title"].str.lower().str.contains(kw, na=False).astype(int)
    
    # Count suspicious keywords
    df["suspicious_keyword_count"] = sum(
        df["title"].str.lower().str.contains(kw, na=False).astype(int)
        for kw in suspicious_keywords
    )
    
    return df


def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract temporal features."""
    df = df.copy()
    
    df["award_month"] = pd.to_datetime(df["award_date"]).dt.month
    df["award_day_of_week"] = pd.to_datetime(df["award_date"]).dt.dayofweek
    df["award_quarter"] = pd.to_datetime(df["award_date"]).dt.quarter
    
    # End of year (budget spending rush)
    df["is_end_of_year"] = df["award_month"].isin([11, 12]).astype(int)
    
    # Monday (common for suspicious decisions)
    df["is_monday"] = (df["award_day_of_week"] == 0).astype(int)
    
    return df


def engineer_all_features(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Run all feature engineering."""
    df = calculate_contract_features(raw_df)
    df = extract_text_features(df)
    df = create_time_features(df)
    
    print(f"🧠 Engineered {len([c for c in df.columns if c not in raw_df])} new features")
    return df


if __name__ == "__main__":
    # Quick test
    from scraper_secop import SecopScraper
    
    scraper = SecopScraper()
    contracts = scraper.fetch_contracts()
    
    featured = engineer_all_features(contracts)
    print(featured[["contract_id", "risk_score", "single_bidder", "vendor_age_days"]].head())