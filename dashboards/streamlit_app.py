"""
Streamlit Dashboard for Corruption Detection - Enhanced Version
Includes detailed contract view with suspicion explanations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.scraper_secop import SecopScraper
from src.features.engineering import engineer_all_features
from src.models.anomaly.isolation_forest import CorruptionDetector
from src.models.network.graph_builder import CorruptionNetwork


@st.cache_data
def load_data():
    """Load and process all data."""
    scraper = SecopScraper("data/raw")
    contracts = scraper.fetch_contracts()
    vendors = scraper.fetch_vendor_registry()
    officials = scraper.fetch_officials()
    
    # Engineer features
    contracts = engineer_all_features(contracts)
    
    # Run anomaly detection
    detector = CorruptionDetector(contamination=0.2)
    detector.fit(contracts)
    contracts = detector.predict(contracts)
    
    # Build network
    network = CorruptionNetwork()
    network.load_data(contracts, vendors, officials)
    network.build_graph()
    communities = network.detect_communities()
    
    return contracts, vendors, officials, detector, network, communities


def explain_suspicion(row: dict) -> list:
    """
    Genera una explicación detallada de por qué un contrato es sospechoso.
    Returns lista de razones con severity.
    """
    reasons = []
    
    # 1. Single bidder
    if row.get("single_bidder", 0) == 1:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Un solo bidder",
            "detail": "Solo una empresa compitió → posible arreglado",
            "rule": "contracts with num_bidders = 1"
        })
    
    # 2. Bid ratio (precio cerca del techo)
    bid_ratio = row.get("bid_ratio", 0)
    if bid_ratio > 0.95:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Precio cerca del máximo",
            "detail": f"El precio ({bid_ratio:.1%}) está muy cerca del techo → posible collusion",
            "rule": "bid_ratio > 0.95"
        })
    elif bid_ratio > 0.85:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Precio elevado",
            "detail": f"El precio ({bid_ratio:.1%}) está por encima del 85% del techo",
            "rule": "bid_ratio > 0.85"
        })
    
    # 3. Vendor age (empresa nueva)
    vendor_age = row.get("vendor_age_days", 9999)
    if vendor_age < 180:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Empresa muy nueva",
            "detail": f"La empresa tiene solo {vendor_age} días → creada justo para ganar este contrato",
            "rule": "vendor_age_days < 180"
        })
    elif vendor_age < 365:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Empresa reciente",
            "detail": f"La empresa tiene menos de 1 año ({vendor_age} días)",
            "rule": "vendor_age_days < 365"
        })
    
    # 4. Contract type risk
    type_risk = row.get("type_risk_score", 0)
    if type_risk > 0.7:
        contract_type = row.get("contract_type", "unknown")
        type_name = {
            "contratacion_directa": "Contratación directa",
            "seleccion_abreviada": "Selección abreviada",
            "minima_cuantia": "Mínima cuantía"
        }.get(contract_type, contract_type)
        
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": f"Tipo de riesgo: {type_name}",
            "detail": "Los contratos directos tienen menos supervisión",
            "rule": f"contract_type = {contract_type}"
        })
    
    # 5. Modifications
    mods = row.get("modifications", 0)
    if mods > 2:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Muchas modificaciones",
            "detail": f"El contrato tuvo {mods} modificaciones →可能最初低价后增加",
            "rule": "modifications > 2"
        })
    elif mods > 0:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Modificaciones",
            "detail": f"El contrato tuvo {mods} modificación(es)",
            "rule": "modifications > 0"
        })
    
    # 6. Suspicious keywords
    keyword_count = row.get("suspicious_keyword_count", 0)
    if keyword_count >= 2:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Palabras sensibles",
            "detail": f"El título contiene {keyword_count} palabras sensibles (consultoría, asesoría, etc.)",
            "rule": "suspicious_keyword_count >= 2"
        })
    
    # 7. End of year
    if row.get("is_end_of_year", 0) == 1:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Fin de año",
            "detail": "Contrato adjudicado en nov-dic → posible rush de gasto presupuestal",
            "rule": "award_month in [11, 12]"
        })
    
    # 8. Rounded number
    if row.get("is_rounded", 0) == 1:
        reasons.append({
            "severity": "🟢 BAJO",
            "factor": "Número redondo",
            "detail": "El valor es redondo (múltiplo de 10M) → posible estimación falta",
            "rule": "contract_value % 10000000 == 0"
        })
    
    return reasons


# Page config
st.set_page_config(page_title="Pereira Corruption Detector", layout="wide")

st.title("🚨 Pereira Corruption Detection System")
st.markdown("AI-powered analysis of public procurement in Pereira, Colombia")

# Load data
try:
    contracts, vendors, officials, detector, network, communities = load_data()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Run the ETL scripts first: python src/etl/scraper_secop.py")
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")

risk_threshold = st.sidebar.slider(
    "Suspicion Score Threshold",
    min_value=0,
    max_value=100,
    value=50
)

show_anomalies_only = st.sidebar.checkbox("Show anomalies only", value=False)

# Filter data
filtered = contracts[contracts["suspicion_score"] >= risk_threshold]
if show_anomalies_only:
    filtered = filtered[filtered["anomaly_label"] == -1]

# --- KPI Row ---
st.markdown("### 📊 Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Contracts", len(contracts))
with col2:
    st.metric("Anomalies Detected", len(contracts[contracts["anomaly_label"] == -1]))
with col3:
    st.metric("High Risk (>70)", len(contracts[contracts["suspicion_score"] > 70]))
with col4:
    st.metric("Total Value", f"${contracts['contract_value'].sum()/1e9:.2f}B")

# --- Suspicious Contracts Table with Detail Link ---
st.markdown("### 🚨 Most Suspicious Contracts")

# Create a selectbox to view contract details
if not filtered.empty:
    # Get top suspicious
    top_suspicious = detector.get_top_suspicious(filtered, n=50)[
        ["contract_id", "title", "vendor", "contract_value", "suspicion_score", "anomaly_label"]
    ]
    
    # Show simple table first
    st.dataframe(
        top_suspicious,
        use_container_width=True,
        column_config={
            "suspicion_score": st.column_config.ProgressColumn(
                "Suspicion Score",
                format="%d",
                min_value=0,
                max_value=100,
            ),
            "contract_value": st.column_config.NumberColumn(
                "Value (COP)",
                format="$%d",
            ),
        }
    )
    
    # --- CONTRACT DETAIL VIEW ---
    st.markdown("---")
    st.markdown("### 🔎 Contract Detail View")
    
    # Select contract to view
    selected_contract_id = st.selectbox(
        "Select a contract to view details:",
        options=filtered.sort_values("suspicion_score", ascending=False)["contract_id"].tolist(),
        index=0
    )
    
    # Get contract details
    contract_row = contracts[contracts["contract_id"] == selected_contract_id].iloc[0]
    
    # Header with score
    score = contract_row["suspicion_score"]
    score_color = "🔴" if score > 70 else "🟡" if score > 50 else "🟢"
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"## 📋 {contract_row['contract_id']}")
    with col2:
        st.markdown(f"<h1 style='text-align: right;'>{score_color} {score:.0f}/100</h1>", unsafe_allow_html=True)
    
    st.markdown(f"**{contract_row['title']}**")
    
    # Contract Info Grid
    st.markdown("#### 📄 Contract Information")
    
    info_col1, info_col2, info_col3, info_col4 = st.columns(4)
    
    with info_col1:
        st.markdown(f"**Vendor:** {contract_row['vendor']}")
        st.markdown(f"**NIT:** {contract_row['vendor_nit']}")
    
    with info_col2:
        st.markdown(f"**Value:** ${contract_row['contract_value']:,.0f}")
        st.markdown(f"**Ceiling:** ${contract_row['ceiling_value']:,.0f}")
    
    with info_col3:
        st.markdown(f"**Type:** {contract_row['contract_type']}")
        st.markdown(f"**Bidders:** {contract_row['num_bidders']}")
    
    with info_col4:
        st.markdown(f"**Date:** {contract_row['award_date']}")
        st.markdown(f"**Modifications:** {contract_row['modifications']}")
    
    # 🚨 SUSPICION ANALYSIS
    st.markdown("---")
    st.markdown("### 🚨 Suspicion Analysis")
    
    reasons = explain_suspicion(contract_row)
    
    if reasons:
        st.markdown(f"**Found {len(reasons)} risk factors:**")
        
        for i, reason in enumerate(reasons, 1):
            with st.expander(f"{reason['severity']} {reason['factor']}", expanded=True):
                st.markdown(f"**Detail:** {reason['detail']}")
                st.caption(f"Rule: {reason['rule']}")
    else:
        st.success("✅ No suspicious factors detected - this contract appears normal")
    
    # Risk Score Breakdown
    st.markdown("#### 📊 Risk Score Breakdown")
    
    # Show individual scores
    score_components = {
        "Single Bidder": contract_row.get("single_bidder", 0) * 25,
        "Price Ratio": (contract_row.get("bid_ratio", 0) if contract_row.get("bid_ratio", 0) > 0.95 else 0) * 20,
        "Vendor Age": (1 if contract_row.get("vendor_age_days", 9999) < 365 else 0) * 20,
        "Type Risk": contract_row.get("type_risk_score", 0) * 15,
        "Modifications": contract_row.get("has_modifications", 0) * 10,
        "Rounded Number": contract_row.get("is_rounded", 0) * 10,
    }
    
    # Bar chart of components
    fig = px.bar(
        x=list(score_components.values()),
        y=list(score_components.keys()),
        orientation='h',
        title="Risk Score Components",
        labels={"x": "Score Contribution", "y": "Factor"},
        color_discrete_sequence=["#FF4B4B"]
    )
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No contracts match the current filters")

# --- Charts ---
st.markdown("### 📈 Analysis")

tab1, tab2, tab3 = st.tabs(["Risk Distribution", "Contract Types", "Vendor Analysis"])

with tab1:
    fig = px.histogram(
        contracts,
        x="suspicion_score",
        nbins=20,
        title="Distribution of Suspicion Scores",
        labels={"suspicion_score": "Suspicion Score"},
        color_discrete_sequence=["#FF4B4B"]
    )
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    fig = px.bar(
        contracts.groupby("contract_type")["suspicion_score"].mean().sort_values(ascending=False),
        title="Average Suspicion Score by Contract Type",
        labels={"value": "Avg Suspicion Score", "contract_type": "Contract Type"},
        color_discrete_sequence=["#3366CC"]
    )
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    vendor_stats = contracts.groupby("vendor").agg({
        "contract_value": "sum",
        "suspicion_score": "mean",
        "contract_id": "count"
    }).rename(columns={"contract_id": "num_contracts"})
    vendor_stats = vendor_stats.sort_values("suspicion_score", ascending=False)
    
    fig = px.scatter(
        vendor_stats,
        x="num_contracts",
        y="suspicion_score",
        size="contract_value",
        hover_name=vendor_stats.index,
        title="Vendor Risk Analysis",
        labels={
            "num_contracts": "Number of Contracts",
            "suspicion_score": "Avg Suspicion Score",
            "contract_value": "Total Value"
        },
        color_discrete_sequence=["#FF4B4B"]
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Network Analysis ---
st.markdown("### 🔗 Network Analysis")

if communities["suspicious"]:
    st.warning(f"Found {len(communities['suspicious'])} suspicious communities!")
    
    for comm in communities["suspicious"]:
        with st.expander(f"Community {comm['community_id']} - {comm['risk_level']} RISK"):
            st.write(f"**Vendors:** {comm['vendor_count']}")
            st.write(f"**Officials:** {comm['official_count']}")
            st.write(f"**Contracts:** {comm['contract_count']}")
            st.write(f"**Total Value:** ${comm['total_value']/1e6:.1f}M")
            st.write("**Nodes:**", ", ".join(comm["nodes"][:10]), "..." if len(comm["nodes"]) > 10 else "")
else:
    st.info("No suspicious communities detected in current data.")

# --- Footer ---
st.markdown("---")
st.caption("🚨 Pereira Corruption Detection Prototype | SECOP Data Analysis")