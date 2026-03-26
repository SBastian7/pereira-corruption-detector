"""
Pereira Corruption Detector - Enhanced Investigative Dashboard
Designed for journalists, auditors, and investigators.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIG & SETUP
# ============================================================================

st.set_page_config(
    page_title="Pereira Corruption Detector",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# DATA LOADING LAYER
# ============================================================================

@st.cache_data
def load_contracts() -> pd.DataFrame:
    """Load processed contracts with suspicion scores."""
    path = Path("data/processed/contracts_scored.csv")
    if not path.exists():
        # Fallback: try to generate from raw + pipeline
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    
    # Handle missing columns with defaults
    defaults = {
        'contract_id': 'unknown',
        'title': 'Untitled',
        'contract_type': 'unknown',
        'municipality': 'Pereira',
        'vendor': 'Unknown',
        'vendor_age_days': 9999,
        'value': 0,
        'date': None,
        'suspicion_score': 0,
        'single_bidder': 0,
        'bid_ratio': 0,
        'type_risk_score': 0,
        'keywords': ''
    }
    
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    
    # Normalize column names
    df = df.rename(columns={'value': 'contract_value'})
    
    # Parse dates
    if 'date' in df.columns and df['date'].dtype == 'object':
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    return df


@st.cache_data
def load_vendors(contracts: pd.DataFrame) -> pd.DataFrame:
    """Extract and aggregate vendor data."""
    if contracts.empty:
        return pd.DataFrame()
    
    vendors = contracts.groupby('vendor').agg({
        'contract_id': 'count',
        'contract_value': ['sum', 'mean'],
        'suspicion_score': 'mean',
        'vendor_age_days': 'first'
    }).reset_index()
    
    vendors.columns = ['vendor', 'contract_count', 'total_value', 'avg_value', 'avg_suspicion', 'vendor_age']
    vendors = vendors.sort_values('avg_suspicion', ascending=False)
    
    return vendors


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def explain_suspicion(row: dict) -> list:
    """Generate explanation of why a contract is suspicious."""
    reasons = []
    
    # Single bidder
    if row.get("single_bidder", 0) == 1:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Un solo bidder",
            "detail": "Solo una empresa compitió → posible arreglado",
            "rule": "single_bidder = 1"
        })
    
    # Bid ratio
    bid_ratio = row.get("bid_ratio", 0)
    if bid_ratio > 0.95:
        reasons.append({
            "severity": "🔴 ALTO",
            "factor": "Precio cerca del máximo",
            "detail": f"El precio ({bid_ratio:.1%}) está muy cerca del techo → posible colusión",
            "rule": "bid_ratio > 0.95"
        })
    elif bid_ratio > 0.85:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Precio elevado",
            "detail": f"El precio ({bid_ratio:.1%}) está por encima del 85% del techo",
            "rule": "bid_ratio > 0.85"
        })
    
    # Vendor age
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
    
    # Contract type risk
    type_risk = row.get("type_risk_score", 0)
    if type_risk > 0.7:
        reasons.append({
            "severity": "🟡 MEDIO",
            "factor": "Tipo de contrato de riesgo",
            "detail": "Contratos directos tienen menos supervisión",
            "rule": f"type_risk_score = {type_risk}"
        })
    
    return reasons


def get_risk_category(score: float) -> str:
    """Categorize suspicion score."""
    if score >= 80:
        return "CRITICAL"
    elif score >= 60:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    else:
        return "LOW"


# ============================================================================
# PAGE: OVERVIEW
# ============================================================================

def page_overview(contracts: pd.DataFrame):
    """Dashboard overview with key metrics and charts."""
    st.markdown("## 📊 Overview")
    
    if contracts.empty:
        st.warning("No data available. Please run the ETL pipeline first.")
        return
    
    # --- KPI Cards ---
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Contracts", len(contracts))
    with col2:
        flagged = len(contracts[contracts['suspicion_score'] > 60])
        st.metric("🚨 Flagged (High+)", flagged)
    with col3:
        unique_vendors = contracts['vendor'].nunique()
        st.metric("Unique Vendors", unique_vendors)
    with col4:
        total_value = contracts['contract_value'].sum() / 1e6
        st.metric("Total Value (M COP)", f"${total_value:,.0f}")
    
    st.markdown("---")
    
    # --- Charts Row 1 ---
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown("### 📈 Suspicion Score Distribution")
        fig = px.histogram(
            contracts,
            x="suspicion_score",
            nbins=10,
            title="Distribution of Suspicion Scores",
            labels={"suspicion_score": "Score", "count": "Count"},
            color_discrete_sequence=["#FF4B4B"]
        )
        fig.update_layout(bargap=0.1)
        st.plotly_chart(fig, use_container_width=True)
    
    with col_chart2:
        st.markdown("### 📋 Contracts by Type")
        type_counts = contracts.groupby('contract_type').size().sort_values(ascending=False)
        fig = px.bar(
            type_counts,
            x=type_counts.index,
            y=type_counts.values,
            title="Contracts by Type",
            labels={"x": "Type", "y": "Count"},
            color_discrete_sequence=["#3366CC"]
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # --- Charts Row 2 ---
    col_chart3, col_chart4 = st.columns(2)
    
    with col_chart3:
        st.markdown("### 💰 Value Distribution by Risk Level")
        contracts['risk_category'] = contracts['suspicion_score'].apply(get_risk_category)
        fig = px.box(
            contracts,
            x="risk_category",
            y="contract_value",
            title="Contract Value by Risk Level",
            labels={"risk_category": "Risk Level", "contract_value": "Value (COP)"},
            category_orders={"risk_category": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
            color_discrete_sequence=["#00CC96", "#FFA15A", "#FF6692", "#FF4B4B"]
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col_chart4:
        st.markdown("### 🔴 Top Risk Factors")
        # Calculate common risk factors
        risk_counts = {
            'Single Bidder': int(contracts['single_bidder'].sum()),
            'New Vendor (<1yr)': int((contracts['vendor_age_days'] < 365).sum()),
            'High Bid Ratio': int((contracts['bid_ratio'] > 0.9).sum()),
            'Type Risk': int((contracts['type_risk_score'] > 0.5).sum())
        }
        risk_df = pd.DataFrame(list(risk_counts.items()), columns=['Factor', 'Count'])
        fig = px.bar(
            risk_df,
            x="Factor",
            y="Count",
            title="Common Risk Factors",
            color_discrete_sequence=["#FF4B4B"]
        )
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# PAGE: CONTRACT EXPLORER
# ============================================================================

def page_explorer(contracts: pd.DataFrame):
    """Main contract table with filtering and search."""
    st.markdown("## 🔍 Contract Explorer")
    
    if contracts.empty:
        st.warning("No data available.")
        return
    
    # --- Sidebar Filters ---
    st.sidebar.markdown("### 🎛️ Filters")
    
    # Date range
    if 'date' in contracts.columns and not contracts['date'].isna().all():
        min_date = contracts['date'].min().date()
        max_date = contracts['date'].max().date()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    else:
        date_range = None
    
    # Value range
    min_val = int(contracts['contract_value'].min())
    max_val = int(contracts['contract_value'].max())
    value_range = st.sidebar.slider(
        "Value Range (COP millions)",
        min_value=0,
        max_value=int(max_val / 1e6) + 1,
        value=(0, int(max_val / 1e6) + 1)
    )
    
    # Suspicion level
    suspicion_options = ["All", "LOW (<40)", "MEDIUM (40-60)", "HIGH (60-80)", "CRITICAL (>80)"]
    suspicion_filter = st.sidebar.selectbox("Suspicion Level", suspicion_options)
    
    # Contract type
    contract_types = ["All"] + sorted(contracts['contract_type'].unique().tolist())
    type_filter = st.sidebar.multiselect("Contract Type", contract_types, default=["All"])
    
    # --- Search ---
    search_query = st.text_input("🔎 Search contracts...", placeholder="Search by title, vendor, or keywords...")
    
    # --- Apply Filters ---
    filtered = contracts.copy()
    
    # Search filter
    if search_query:
        search_lower = search_query.lower()
        filtered = filtered[
            filtered['title'].str.lower().str.contains(search_lower, na=False) |
            filtered['vendor'].str.lower().str.contains(search_lower, na=False) |
            filtered['keywords'].str.lower().str.contains(search_lower, na=False)
        ]
    
    # Date filter
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        filtered = filtered[
            (filtered['date'].dt.date >= start_date) &
            (filtered['date'].dt.date <= end_date)
        ]
    
    # Value filter
    filtered = filtered[
        (filtered['contract_value'] >= value_range[0] * 1e6) &
        (filtered['contract_value'] <= value_range[1] * 1e6)
    ]
    
    # Suspicion filter
    if suspicion_filter != "All":
        if "LOW" in suspicion_filter:
            filtered = filtered[filtered['suspicion_score'] < 40]
        elif "MEDIUM" in suspicion_filter:
            filtered = filtered[(filtered['suspicion_score'] >= 40) & (filtered['suspicion_score'] < 60)]
        elif "HIGH" in suspicion_filter:
            filtered = filtered[(filtered['suspicion_score'] >= 60) & (filtered['suspicion_score'] < 80)]
        elif "CRITICAL" in suspicion_filter:
            filtered = filtered[filtered['suspicion_score'] >= 80]
    
    # Type filter
    if "All" not in type_filter and type_filter:
        filtered = filtered[filtered['contract_type'].isin(type_filter)]
    
    # --- Results Count ---
    st.markdown(f"**Showing {len(filtered)} of {len(contracts)} contracts**")
    
    # --- Data Table with Pagination ---
    if not filtered.empty:
        # Pagination controls
        page_size = 25
        total_pages = max(1, (len(filtered) + page_size - 1) // page_size)
        page_num = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            label_visibility="collapsed"
        )
        st.caption(f"Page {page_num} of {total_pages}")
        
        # Paginate
        start_idx = (page_num - 1) * page_size
        end_idx = min(start_idx + page_size, len(filtered))
        page_df = filtered.iloc[start_idx:end_idx]
        
        # Select columns to display
        display_cols = ['contract_id', 'title', 'vendor', 'contract_type', 'contract_value', 'date', 'suspicion_score']
        display_df = page_df[display_cols].copy()
        
        # Format for display
        display_df['contract_value'] = display_df['contract_value'].apply(lambda x: f"${x/1e6:.1f}M")
        display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
        
        # Sort by suspicion
        display_df = display_df.sort_values('suspicion_score', ascending=False)
        
        # Show with configuration
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400,
            column_config={
                "suspicion_score": st.column_config.ProgressColumn(
                    "Score",
                    format="%d",
                    min_value=0,
                    max_value=100,
                ),
            }
        )
        
        # --- Contract Detail ---
        st.markdown("---")
        st.markdown("### 🔎 Contract Detail")
        
        selected_id = st.selectbox(
            "Select a contract to view details:",
            options=filtered.sort_values('suspicion_score', ascending=False)['contract_id'].tolist()
        )
        
        if selected_id:
            row = filtered[filtered['contract_id'] == selected_id].iloc[0]
            
            # Header
            score = row['suspicion_score']
            score_emoji = "🔴" if score >= 80 else "🟡" if score >= 60 else "🟢"
            
            col_d1, col_d2 = st.columns([3, 1])
            with col_d1:
                st.markdown(f"### 📋 {row['contract_id']}")
                st.markdown(f"**{row['title']}**")
            with col_d2:
                st.markdown(f"<h1 style='text-align: right;'>{score_emoji} {score:.0f}</h1>", unsafe_allow_html=True)
            
            # Info grid
            info_cols = st.columns(4)
            with info_cols[0]:
                st.markdown("**Vendor:**")
                st.markdown(row['vendor'])
            with info_cols[1]:
                st.markdown("**Value:**")
                st.markdown(f"${row['contract_value']:,.0f}")
            with info_cols[2]:
                st.markdown("**Type:**")
                st.markdown(row['contract_type'])
            with info_cols[3]:
                st.markdown("**Date:**")
                st.markdown(str(row['date'])[:10] if pd.notna(row['date']) else 'N/A')
            
            # Suspicion explanation
            reasons = explain_suspicion(row.to_dict())
            
            if reasons:
                st.markdown("#### 🚨 Risk Factors")
                for reason in reasons:
                    with st.expander(f"{reason['severity']} {reason['factor']}", expanded=True):
                        st.markdown(f"**Detail:** {reason['detail']}")
                        st.caption(f"Rule: {reason['rule']}")
            else:
                st.success("✅ No suspicious factors detected")
    
    else:
        st.info("No contracts match the current filters.")


# ============================================================================
# PAGE: ENTITY LOOKUP
# ============================================================================

def page_entities(contracts: pd.DataFrame, vendors: pd.DataFrame):
    """Search and view vendor/official profiles."""
    st.markdown("## 🔎 Entity Lookup")
    
    if contracts.empty:
        st.warning("No data available.")
        return
    
    # Search
    search = st.text_input("🔍 Search vendors...", placeholder="Type vendor name...")
    
    # Filter vendors
    if search:
        vendor_results = vendors[vendors['vendor'].str.lower().str.contains(search.lower())]
    else:
        vendor_results = vendors.head(20)
    
    # Results
    if not vendor_results.empty:
        st.markdown(f"**Found {len(vendor_results)} vendors**")
        
        # Summary table
        display_vendors = vendor_results[['vendor', 'contract_count', 'total_value', 'avg_suspicion', 'vendor_age']].copy()
        display_vendors['total_value'] = display_vendors['total_value'].apply(lambda x: f"${x/1e6:.1f}M")
        display_vendors['avg_suspicion'] = display_vendors['avg_suspicion'].apply(lambda x: f"{x:.0f}")
        
        st.dataframe(
            display_vendors,
            use_container_width=True,
            height=400
        )
        
        # --- Vendor Detail ---
        st.markdown("---")
        st.markdown("### 📋 Vendor Profile")
        
        selected_vendor = st.selectbox(
            "Select a vendor to view details:",
            options=vendor_results['vendor'].tolist()
        )
        
        if selected_vendor:
            # Get vendor contracts
            vendor_contracts = contracts[contracts['vendor'] == selected_vendor]
            
            col_v1, col_v2, col_v3 = st.columns(3)
            with col_v1:
                st.metric("Total Contracts", len(vendor_contracts))
            with col_v2:
                st.metric("Total Value", f"${vendor_contracts['contract_value'].sum()/1e6:.1f}M")
            with col_v3:
                st.metric("Avg Suspicion", f"{vendor_contracts['suspicion_score'].mean():.0f}")
            
            st.markdown("#### Contract History")
            
            history_df = vendor_contracts[['contract_id', 'title', 'contract_value', 'date', 'suspicion_score']].copy()
            history_df['contract_value'] = history_df['contract_value'].apply(lambda x: f"${x/1e6:.1f}M")
            history_df['date'] = pd.to_datetime(history_df['date']).dt.strftime('%Y-%m-%d')
            
            st.dataframe(history_df, use_container_width=True)
    else:
        st.info("No vendors found.")


# ============================================================================
# PAGE: EXPORT
# ============================================================================

def page_export(contracts: pd.DataFrame):
    """Export functionality."""
    st.markdown("## 📥 Export Data")
    
    if contracts.empty:
        st.warning("No data available to export.")
        return
    
    # Export options
    st.markdown("### Select Export Options")
    
    export_type = st.radio(
        "Export Scope:",
        ["Current Filtered View", "Full Dataset"],
        horizontal=True
    )
    
    # Column selection
    all_columns = contracts.columns.tolist()
    selected_columns = st.multiselect(
        "Select columns to export:",
        all_columns,
        default=all_columns
    )
    
    if selected_columns:
        # Prepare data
        export_df = contracts[selected_columns]
        
        # Convert to CSV
        csv = export_df.to_csv(index=False)
        
        # Download button
        st.download_button(
            label=f"📥 Download {export_type} as CSV",
            data=csv,
            file_name=f"contracts_export_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv"
        )
        
        st.markdown("#### Preview")
        st.dataframe(export_df.head(10), use_container_width=True)
    else:
        st.warning("Please select at least one column.")


# ============================================================================
# MAIN APP
# ============================================================================

def main():
    """Main application entry point."""
    
    # Load data
    contracts = load_contracts()
    vendors = load_vendors(contracts)
    
    # Sidebar navigation
    st.sidebar.markdown("# 🚨 Navigation")
    pages = {
        "Overview": page_overview,
        "Contract Explorer": page_explorer,
        "Entity Lookup": page_entities,
        "Export": page_export
    }
    
    selected_page = st.sidebar.radio("Go to:", list(pages.keys()))
    
    # Run selected page
    if selected_page == "Entity Lookup":
        pages[selected_page](contracts, vendors)
    else:
        pages[selected_page](contracts)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.caption("🚨 Pereira Corruption Detector | Investigative Dashboard")
    
    # Data refresh note
    st.sidebar.markdown("---")
    st.sidebar.info("💡 To refresh data, rerun the pipeline: `python main.py --stage=all`")


if __name__ == "__main__":
    main()