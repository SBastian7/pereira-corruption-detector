# 🚨 Pereira Corruption Detection System

AI-powered system to detect bribery and corruption patterns in public procurement data from Pereira, Colombia.

## Overview

This system analyzes municipal contracts to identify suspicious patterns that may indicate corruption. It uses machine learning (Isolation Forest) and network analysis to detect anomalies and potential corruption rings.

## Features

- 📥 **Automated ETL** - Fetch data from SECOP (Colombia's public procurement platform)
- 🧠 **Anomaly Detection** - ML-based identification of suspicious contracts
- 🔗 **Network Analysis** - Graph-based detection of corruption rings
- 📊 **Interactive Dashboard** - Streamlit visualization with detailed contract views

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run ETL (fetch real data)
python main.py --stage=etl

# Or download from SECOP open data portal
python main.py --stage=download

# Run full pipeline
python main.py --stage=all

# Launch dashboard
streamlit run dashboards/streamlit_app.py
```

## Project Structure

```
pereira-corruption-detector/
├── main.py                     # Pipeline runner
├── requirements.txt            # Dependencies
├── src/
│   ├── etl/
│   │   ├── scraper_secop.py   # SECOP API + mock data
│   │   └── download_secop_data.py  # Portal downloader
│   ├── features/
│   │   └── engineering.py     # Feature extraction
│   └── models/
│       ├── anomaly/
│       │   └── isolation_forest.py  # ML detector
│       └── network/
│           └── graph_builder.py     # Network analysis
└── dashboards/
    └── streamlit_app.py        # Interactive UI
```

## How It Works

### 1. Data Collection
Fetches contracts from SECOP II API or downloads from the public data portal.

### 2. Feature Engineering
Extracts signals such as:
- Single bidder contracts
- Price close to ceiling
- Vendor age (new companies)
- Contract type risk
- Suspicious keywords

### 3. Anomaly Detection
Uses Isolation Forest to identify contracts that deviate from "normal" patterns.

### 4. Network Analysis
Builds relationship graphs between officials, vendors, and contracts to detect corruption rings.

## Data Sources

- [SECOP II](https://www.colombiacompra.gov.co/secop) - Public procurement data
- [RUES](https://www.rues.org.co) - Business registry

## License

MIT

## Author

Built with 🔍 for transparency in public procurement.