#!/usr/bin/env python3
"""
Main entry point for Pereira Corruption Detection Pipeline
Run stages: etl -> features -> train -> dashboard
"""

import argparse
import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.etl.scraper_secop import SecopScraper, RealSecopScraper
from src.etl.download_secop_data import SecopDataDownloader
from src.features.engineering import engineer_all_features
from src.models.anomaly.isolation_forest import CorruptionDetector
from src.models.network.graph_builder import CorruptionNetwork


def stage_etl():
    """Extract and load raw data."""
    print("\n" + "="*50)
    print("STAGE 1: ETL - Extracting Data")
    print("="*50)
    
    # Try to get REAL data first
    print("\n🌐 Attempting to fetch REAL data from SECOP...")
    try:
        scraper = RealSecopScraper()
        contracts = scraper.get_contracts_by_municipality("Pereira", 2024, limit=2000)
        print(f"✅ Obtenidos {len(contracts)} contratos reales")
    except Exception as e:
        print(f"⚠️ Error obteniendo datos reales: {e}")
        print("📦 Falling back to mock data...")
        scraper = SecopScraper()
        contracts = scraper.fetch_contracts(municipality="Pereira", year=2024)
    
    vendors = scraper.fetch_vendor_registry() if hasattr(scraper, 'fetch_vendor_registry') else pd.DataFrame()
    officials = scraper.fetch_officials() if hasattr(scraper, 'fetch_officials') else pd.DataFrame()
    
    print(f"\n✅ Loaded {len(contracts)} contracts")
    if not vendors.empty:
        print(f"✅ Loaded {len(vendors)} vendors")
    if not officials.empty:
        print(f"✅ Loaded {len(officials)} officials")
    
    return contracts, vendors, officials


def stage_download():
    """Download raw SECOP data files."""
    print("\n" + "="*50)
    print("DOWNLOAD: Fetching SECOP Open Data")
    print("="*50)
    
    downloader = SecopDataDownloader()
    df = downloader.download_all_years(2020, 2024)
    
    return df


def stage_features(contracts):
    """Engineer features."""
    print("\n" + "="*50)
    print("STAGE 2: Feature Engineering")
    print("="*50)
    
    featured = engineer_all_features(contracts)
    
    # Save processed data
    featured.to_csv("data/processed/contracts_featured.csv", index=False)
    print("\n✅ Saved featured contracts to data/processed/")
    
    return featured


def stage_train(contracts):
    """Train anomaly detection models."""
    print("\n" + "="*50)
    print("STAGE 3: ML Training")
    print("="*50)
    
    # Train Isolation Forest
    detector = CorruptionDetector(contamination=0.15)
    detector.fit(contracts)
    results = detector.predict(contracts)
    
    # Print summary
    print("\n📊 Model Summary:")
    for k, v in detector.summary(results).items():
        print(f"  {k}: {v}")
    
    # Show top suspicious
    print("\n🚨 Top 5 Most Suspicious Contracts:")
    top = detector.get_top_suspicious(results, n=5)
    for _, row in top.iterrows():
        print(f"  {row['contract_id']}: {row['title'][:50]}... | Score: {row['suspicion_score']:.1f}")
    
    # Save results
    results.to_csv("data/processed/contracts_scored.csv", index=False)
    print("\n✅ Saved scored contracts")
    
    return detector, results


def stage_network(contracts, vendors, officials):
    """Build and analyze network."""
    print("\n" + "="*50)
    print("STAGE 4: Network Analysis")
    print("="*50)
    
    network = CorruptionNetwork()
    network.load_data(contracts, vendors, officials)
    network.build_graph()
    communities = network.detect_communities()
    
    print(f"\n🔍 Found {len(communities['all'])} communities")
    print(f"🚨 {len(communities['suspicious'])} suspicious communities")
    
    # Export for visualization
    network.export_for_gephi("data/processed/network.gexf")
    
    return network, communities


def main():
    parser = argparse.ArgumentParser(description="Pereira Corruption Detection Pipeline")
    parser.add_argument(
        "--stage",
        choices=["etl", "features", "train", "network", "download", "all"],
        default="all",
        help="Pipeline stage to run"
    )
    
    args = parser.parse_args()
    
    if args.stage == "etl":
        stage_etl()
    
    elif args.stage == "download":
        stage_download()
    
    elif args.stage == "features":
        contracts = pd.read_csv("data/raw/contracts_Pereira_2024.csv")
        stage_features(contracts)
    
    elif args.stage == "train":
        contracts = pd.read_csv("data/processed/contracts_featured.csv")
        stage_train(contracts)
    
    elif args.stage == "network":
        # Need raw data
        scraper = SecopScraper()
        contracts = scraper.fetch_contracts()
        vendors = scraper.fetch_vendor_registry()
        officials = scraper.fetch_officials()
        contracts = engineer_all_features(contracts)
        stage_network(contracts, vendors, officials)
    
    elif args.stage == "all":
        print("\n🚀 Running Full Pipeline")
        
        # Stage 1: ETL
        contracts, vendors, officials = stage_etl()
        
        # Stage 2: Features
        contracts = stage_features(contracts)
        
        # Stage 3: Train
        detector, results = stage_train(contracts)
        
        # Stage 4: Network
        network, communities = stage_network(contracts, vendors, officials)
        
        print("\n" + "="*50)
        print("✅ PIPELINE COMPLETE")
        print("="*50)
        print("\nNext steps:")
        print("  - Run dashboard: streamlit run dashboards/streamlit_app.py")
        print("  - Open Gephi: load data/processed/network.gexf")
        print("  - Review scored contracts in data/processed/")


if __name__ == "__main__":
    main()