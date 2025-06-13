import pathlib
import polars as pl
import streamlit as st
import plotly.express as px
import pandas as pd

CUR_DIR = pathlib.Path("data/curated")
KPI_FILE = CUR_DIR / "kpi_daily.parquet"
SVC_FILE = CUR_DIR / "svc_daily.parquet"

# --- Load data ---
df = pl.read_parquet(KPI_FILE).sort("usage_date")
df_pd = df.to_pandas()                       # Plotly wants pandas
svc_df = pl.read_parquet(SVC_FILE).sort("usage_date")
svc_pd = svc_df.to_pandas()

# --- Date range filter ---
min_d, max_d = df_pd["usage_date"].min(), df_pd["usage_date"].max()
# Convert to native Python datetime for Streamlit slider
min_d = pd.to_datetime(min_d).to_pydatetime()
max_d = pd.to_datetime(max_d).to_pydatetime()

start_d, end_d = st.slider(
    "Select date range",
    min_value=min_d,
    max_value=max_d,
    value=(min_d, max_d),
    format="YYYY-MM-DD",
)

# Apply filter (convert slider output back to pandas Timestamp for comparison)
mask = (df_pd["usage_date"] >= pd.Timestamp(start_d)) & (
    df_pd["usage_date"] <= pd.Timestamp(end_d))
df_pd = df_pd.loc[mask]
svc_pd = svc_pd.loc[(svc_pd["usage_date"] >= pd.Timestamp(start_d)) & (
    svc_pd["usage_date"] <= pd.Timestamp(end_d))]

st.title("Cloud-Cost & Usage Observatory")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader("Total daily cost (USD)")
    fig = px.line(
        df_pd, x="usage_date", y="total_cost_usd",
        labels={"usage_date": "Date", "total_cost_usd": "Cost (USD)"},
        height=350
    )
    anoms = df_pd[df_pd["is_anomaly"]]
    fig.add_scatter(
        x=anoms["usage_date"],
        y=anoms["total_cost_usd"],
        mode="markers",
        marker=dict(size=8, color="red"),
        name="Anomaly"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    latest = df_pd.iloc[-1]
    st.metric("Latest daily cost", f"${latest.total_cost_usd:,.2f}")
    st.metric("Idle % (latest)", f"{latest.idle_pct*100:.1f}%")
    st.metric("30-day forecast", f"${latest.forecast_30d:,.0f}")


# --- Cost by service stacked area chart ---
service_cols = [c for c in svc_pd.columns if c != "usage_date"]

if service_cols and not svc_pd.empty:
    st.subheader("Cost by service")
    # Convert wide service table to long format for plotly
    svc_long = svc_pd.melt(id_vars="usage_date", var_name="service", value_name="cost_usd")
    area = px.area(
        svc_long,
        x="usage_date",
        y="cost_usd",
        color="service",
        labels={"cost_usd": "USD", "service": "Service"},
        height=300,
    )
    st.plotly_chart(area, use_container_width=True)
else:
    st.info("No service-level data available for the selected date range.")

st.subheader("Idle % scatter")
scatter = px.scatter(
    df_pd,
    x="usage_date",
    y="idle_pct",
    labels={"idle_pct": "Idle ratio"},
    height=250,
)
st.plotly_chart(scatter, use_container_width=True)

st.caption("Demo data • Synthetic or live AWS CE • Built with Polars + Streamlit")
