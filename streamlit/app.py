"""
Fintech Enterprise BI Dashboard — Streamlit
================================
Local BI layer for the Fintech Enterprise AWS Data Engineering Portfolio Project.

Data source priority:
  1. Redshift Serverless (production)
  2. Local Parquet files in data/curated/ (dev fallback)
  3. Raw CSVs in data/raw/ (demo mode)

Run: streamlit run app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Fintech Enterprise Data Pipeline — Analytics",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Data loading ──────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CURATED_DIR = PROJECT_ROOT / "data" / "curated"

# Static FX rates for demo mode
FX_TO_USD = {"AED": 0.2723, "USD": 1.0, "EGP": 0.0206, "GBP": 1.27, "EUR": 1.08}


@st.cache_data(ttl=300)
def load_transactions() -> pd.DataFrame:
    """Load transactions from curated Parquet or raw CSV fallback."""
    curated = CURATED_DIR / "transactions"
    if curated.exists():
        df = pd.read_parquet(curated)
    else:
        df = pd.read_csv(RAW_DIR / "transactions.csv")
        df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
        df["billing_amount"] = pd.to_numeric(df["billing_amount"], errors="coerce")
        df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")
        df["year"] = df["transaction_timestamp"].dt.year.astype(str)
        df["month"] = df["transaction_timestamp"].dt.month.map(lambda m: f"{m:02d}")
        # Compute USD amount
        df["usd_amount"] = df.apply(
            lambda r: r["billing_amount"] * FX_TO_USD.get(str(r.get("billing_currency", "AED")), 0.2723),
            axis=1,
        )
        df["merchant_normalized"] = df["merchant"].str.strip().str.title()
    return df


@st.cache_data(ttl=300)
def load_cards() -> pd.DataFrame:
    curated = CURATED_DIR / "cards"
    if curated.exists():
        return pd.read_parquet(curated)
    df = pd.read_csv(RAW_DIR / "cards.csv")
    df["card_type"] = df["card_type"].str.strip().str.upper()
    df["status"] = df["status"].str.strip().str.upper()
    return df


@st.cache_data(ttl=300)
def load_orgs() -> pd.DataFrame:
    curated = CURATED_DIR / "orgs"
    if curated.exists():
        return pd.read_parquet(curated)
    return pd.read_csv(RAW_DIR / "orgs.csv")


# ── Sidebar ───────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Fintech Enterprise Analytics")
    st.caption("AWS Data Engineering Portfolio")
    st.divider()

    data_source = st.radio(
        "Data source",
        options=["Local CSV / Parquet", "Redshift Serverless"],
        index=0,
    )

    if data_source == "Redshift Serverless":
        st.info("Set REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD env vars to connect")

    st.divider()
    st.markdown("**Architecture**")
    st.markdown(
        "S3 Raw → Glue ETL → S3 Curated (Parquet) → Athena / Redshift → Streamlit",
        help="10-stage AWS data pipeline",
    )

# ── Load data ─────────────────────────────────────────────────────────────────────

try:
    txn_df = load_transactions()
    cards_df = load_cards()
    orgs_df = load_orgs()
    data_loaded = True
except FileNotFoundError as e:
    st.error(f"Data files not found: {e}\n\nPlace CSV files in `data/raw/` to run in demo mode.")
    st.stop()
    data_loaded = False

# ── Header ────────────────────────────────────────────────────────────────────────

st.title("Fintech Enterprise Corporate Card — Transaction Analytics")
st.caption("Designed for enterprise-scale fintech transaction processing. Demonstrated with anonymized sample dataset.")

settled = txn_df[txn_df["status"] == "SETTLED"]
total_spend_aed = settled["billing_amount"].sum()
total_txns = len(txn_df)
settled_txns = len(settled)
declined_txns = len(txn_df[txn_df["status"] == "DECLINED"])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Spend (AED)", f"AED {total_spend_aed:,.0f}")
col2.metric("Total Transactions", f"{total_txns:,}")
col3.metric("Settled", f"{settled_txns:,}", f"{100*settled_txns/max(total_txns,1):.1f}%")
col4.metric("Declined", f"{declined_txns:,}", f"-{100*declined_txns/max(total_txns,1):.1f}%")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Spend by Org",
    "Volume Over Time",
    "FX Exposure",
    "Top Merchants",
    "Card Utilization",
    "Settlement Lag",
])

# ── Tab 1: Spend by Org ───────────────────────────────────────────────────────────

with tab1:
    st.subheader("Total Spend by Organization")

    if "card_id" in txn_df.columns and "id" in cards_df.columns and "id" in orgs_df.columns:
        cards_org = cards_df[["id", "organization_id"]].rename(columns={"id": "card_id"})
        cards_org["card_id"] = cards_org["card_id"].astype(str)
        orgs_merge = orgs_df[["id", "business_name", "country"]].rename(columns={"id": "org_id"})
        orgs_merge["org_id"] = orgs_merge["org_id"].astype(str)
        cards_org["organization_id"] = cards_org["organization_id"].astype(str)
        settled_copy = settled.copy()
        settled_copy["card_id"] = settled_copy["card_id"].astype(str)
        merged = settled_copy.merge(cards_org, on="card_id", how="left")
        merged = merged.merge(orgs_merge, left_on="organization_id", right_on="org_id", how="left")
        merged["business_name"] = merged["business_name"].fillna("Unknown Org")
    else:
        merged = settled.copy()
        merged["business_name"] = "Unknown Org"

    org_spend = (
        merged.groupby("business_name")["billing_amount"]
        .sum()
        .reset_index()
        .sort_values("billing_amount", ascending=False)
        .head(15)
    )

    fig = px.bar(
        org_spend,
        x="billing_amount",
        y="business_name",
        orientation="h",
        labels={"billing_amount": "Total Spend (AED)", "business_name": "Organization"},
        color="billing_amount",
        color_continuous_scale="Blues",
        title="Top 15 Organizations by Card Spend (AED)",
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False, height=500)
    st.plotly_chart(fig, use_container_width=True)

# ── Tab 2: Volume Over Time ───────────────────────────────────────────────────────

with tab2:
    st.subheader("Transaction Volume Over Time")

    monthly = (
        txn_df.groupby(["year", "month"])
        .agg(
            txn_count=("id", "count"),
            total_aed=("billing_amount", "sum"),
        )
        .reset_index()
    )
    monthly["period"] = monthly["year"] + "-" + monthly["month"]
    monthly = monthly.sort_values("period")

    fig_line = px.line(
        monthly,
        x="period",
        y="txn_count",
        markers=True,
        title="Monthly Transaction Count",
        labels={"period": "Month", "txn_count": "Transactions"},
    )
    st.plotly_chart(fig_line, use_container_width=True)

    fig_bar = px.bar(
        monthly,
        x="period",
        y="total_aed",
        title="Monthly Spend Volume (AED)",
        labels={"period": "Month", "total_aed": "Total AED"},
        color="total_aed",
        color_continuous_scale="Teal",
    )
    fig_bar.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

# ── Tab 3: FX Exposure ────────────────────────────────────────────────────────────

with tab3:
    st.subheader("Foreign Exchange Exposure")

    currency_col = "transaction_currency" if "transaction_currency" in settled.columns else "billing_currency"
    fx_data = (
        settled.groupby(currency_col)["billing_amount"]
        .sum()
        .reset_index()
        .rename(columns={currency_col: "currency", "billing_amount": "total_aed"})
        .sort_values("total_aed", ascending=False)
    )
    fx_data["exposure_type"] = fx_data["currency"].map(
        lambda c: "Domestic (AED)" if c == "AED" else "Foreign Currency"
    )

    fig_pie = px.pie(
        fx_data,
        values="total_aed",
        names="currency",
        title="Spend by Transaction Currency (AED equivalent)",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4,
    )
    st.plotly_chart(fig_pie, use_container_width=True)
    st.dataframe(fx_data.round(2), use_container_width=True)

# ── Tab 4: Top Merchants ──────────────────────────────────────────────────────────

with tab4:
    st.subheader("Top 10 Merchants by Spend")

    merchant_col = "merchant_normalized" if "merchant_normalized" in settled.columns else "merchant"
    top_merchants = (
        settled.groupby(merchant_col)["billing_amount"]
        .sum()
        .reset_index()
        .rename(columns={merchant_col: "merchant", "billing_amount": "total_aed"})
        .sort_values("total_aed", ascending=True)
        .tail(10)
    )

    fig_merch = px.bar(
        top_merchants,
        x="total_aed",
        y="merchant",
        orientation="h",
        title="Top 10 Merchants by Total Spend (AED)",
        labels={"total_aed": "Total Spend (AED)", "merchant": "Merchant"},
        color="total_aed",
        color_continuous_scale="Oryel",
    )
    fig_merch.update_layout(coloraxis_showscale=False, height=450)
    st.plotly_chart(fig_merch, use_container_width=True)

# ── Tab 5: Card Utilization ───────────────────────────────────────────────────────

with tab5:
    st.subheader("Card Portfolio Utilization")

    total_cards = len(cards_df)
    active_cards = len(cards_df[cards_df["status"] == "ACTIVE"])
    terminated_cards = len(cards_df[cards_df["status"] == "TERMINATED"])
    virtual_cards = len(cards_df[cards_df["card_type"] == "VIRTUAL"])
    physical_cards = len(cards_df[cards_df["card_type"] == "PHYSICAL"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Cards", total_cards)
    c2.metric("Active", active_cards, f"{100*active_cards/max(total_cards,1):.1f}%")
    c3.metric("Terminated", terminated_cards)

    # Gauge chart
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=100 * active_cards / max(total_cards, 1),
        title={"text": "Active Card Rate (%)"},
        delta={"reference": 80},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#2ecc71"},
            "steps": [
                {"range": [0, 50], "color": "#e74c3c"},
                {"range": [50, 80], "color": "#f39c12"},
                {"range": [80, 100], "color": "#27ae60"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 4},
                "thickness": 0.75,
                "value": 80,
            },
        },
    ))
    st.plotly_chart(fig_gauge, use_container_width=True)

    card_type_fig = px.pie(
        values=[virtual_cards, physical_cards],
        names=["Virtual", "Physical"],
        title="Card Type Distribution",
        color_discrete_map={"Virtual": "#3498db", "Physical": "#e67e22"},
    )
    st.plotly_chart(card_type_fig, use_container_width=True)

# ── Tab 6: Settlement Lag ─────────────────────────────────────────────────────────

with tab6:
    st.subheader("Settlement Lag Distribution")

    if "clearing_timestamp" in txn_df.columns and "transaction_timestamp" in txn_df.columns:
        lag_df = settled.copy()
        lag_df["transaction_timestamp"] = pd.to_datetime(lag_df["transaction_timestamp"], errors="coerce")
        lag_df["clearing_timestamp"] = pd.to_datetime(lag_df["clearing_timestamp"], errors="coerce")
        lag_df["lag_hours"] = (
            (lag_df["clearing_timestamp"] - lag_df["transaction_timestamp"]).dt.total_seconds() / 3600
        )
        lag_df = lag_df[lag_df["lag_hours"].between(0, 240)]  # Filter outliers (0–10 days)

        fig_hist = px.histogram(
            lag_df,
            x="lag_hours",
            nbins=40,
            title="Settlement Lag Distribution (Hours from Transaction to Clearing)",
            labels={"lag_hours": "Hours to Clear", "count": "# Transactions"},
            color_discrete_sequence=["#9b59b6"],
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Median Lag", f"{lag_df['lag_hours'].median():.1f}h")
        col_b.metric("P95 Lag", f"{lag_df['lag_hours'].quantile(0.95):.1f}h")
        col_c.metric("Max Lag", f"{lag_df['lag_hours'].max():.1f}h")
    else:
        st.info("Settlement lag requires both transaction_timestamp and clearing_timestamp columns.")

# ── Footer ────────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Fintech Enterprise AWS Data Engineering Portfolio · "
    "Stack: S3 · Glue · Athena · Redshift Serverless · Lambda · Airflow · Terraform"
)
