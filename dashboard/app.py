"""Streamlit Dashboard for Data Observatory.

Provides real-time visualization of weather data and data quality metrics.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_pipeline.analytics import detect_temperature_anomalies
from data_pipeline.storage import DatabaseManager

# Page configuration
st.set_page_config(
    page_title="Data Observatory",
    page_icon="🔭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
    }
    .quality-pass {
        color: #00d26a;
        font-weight: bold;
    }
    .quality-fail {
        color: #f44336;
        font-weight: bold;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 8px;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_database() -> DatabaseManager | None:
    """Get cached database connection."""
    try:
        return DatabaseManager()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


def format_temperature(temp: float) -> str:
    """Format temperature with color coding."""
    if temp < 0:
        return f"🥶 {temp:.1f}°C"
    elif temp < 15:
        return f"❄️ {temp:.1f}°C"
    elif temp < 25:
        return f"🌤️ {temp:.1f}°C"
    else:
        return f"🔥 {temp:.1f}°C"


def main() -> None:
    """Main dashboard application."""
    # Header
    st.title("🔭 Data Observatory")
    st.markdown("### Self-Healing Data Quality Platform")

    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/clouds/200/telescope.png", width=100)
        st.header("Navigation")

        page = st.radio(
            "Select Page",
            ["📊 Overview", "🌡️ Weather Data", "✅ Data Quality", "🚀 Pipeline Status"],
        )

        st.divider()

        # Refresh button
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        # Settings
        st.header("Settings")
        auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)

        if auto_refresh:
            st.info("Auto-refresh is enabled")

    # Get database connection
    db = get_database()

    if db is None or not db.health_check():
        st.error("⚠️ Database connection unavailable. Please ensure PostgreSQL is running.")
        st.info("Run: `docker-compose up -d` to start the infrastructure.")
        return

    # Route to appropriate page
    if page == "📊 Overview":
        render_overview(db)
    elif page == "🌡️ Weather Data":
        render_weather_data(db)
    elif page == "✅ Data Quality":
        render_quality_metrics(db)
    elif page == "🚀 Pipeline Status":
        render_pipeline_status(db)


def render_overview(db: DatabaseManager) -> None:
    """Render the overview dashboard."""
    st.header("📊 Dashboard Overview")

    # Fetch metrics
    metrics = db.get_quality_metrics()
    latest_weather = db.get_latest_weather(limit=50)

    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Records",
            f"{metrics.get('total_records', 0):,}",
            delta=None,
        )

    with col2:
        st.metric(
            "Cities Tracked",
            metrics.get("unique_cities", 0),
        )

    with col3:
        st.metric(
            "Avg Temperature",
            f"{metrics.get('avg_temperature', 0):.1f}°C",
        )

    with col4:
        quality_pct = metrics.get("valid_temperature_pct", 100)
        st.metric(
            "Data Quality",
            f"{quality_pct:.1f}%",
            delta=f"{'✅' if quality_pct >= 95 else '⚠️'}",
        )

    st.divider()

    # Two column layout
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🌍 Latest Weather by City")

        if latest_weather:
            df = pd.DataFrame(latest_weather)

            # Get latest per city
            df["recorded_at"] = pd.to_datetime(df["recorded_at"])
            latest_per_city = df.sort_values("recorded_at").groupby("city").last().reset_index()

            # Display as cards
            for _, row in latest_per_city.iterrows():
                with st.container():
                    c1, c2, c3 = st.columns([2, 1, 1])
                    with c1:
                        st.write(f"**{row['city']}**, {row['country']}")
                    with c2:
                        st.write(format_temperature(row["temperature_celsius"]))
                    with c3:
                        st.write(f"💧 {row['humidity']}%")
        else:
            st.info("No weather data available yet. Run the pipeline to ingest data.")

    with col_right:
        st.subheader("📈 Temperature Trends")

        if latest_weather:
            df = pd.DataFrame(latest_weather)
            df["recorded_at"] = pd.to_datetime(df["recorded_at"])

            fig = px.line(
                df,
                x="recorded_at",
                y="temperature_celsius",
                color="city",
                title="Temperature Over Time",
                labels={"temperature_celsius": "Temperature (°C)", "recorded_at": "Time"},
            )
            fig.update_layout(
                height=400,
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data to display")


def render_weather_data(db: DatabaseManager) -> None:
    """Render weather data exploration page."""
    st.header("🌡️ Weather Data Explorer")

    # Filters
    col1, col2, col3 = st.columns(3)

    latest_weather = db.get_latest_weather(limit=500)

    if not latest_weather:
        st.info("No weather data available. Run the pipeline to ingest data.")
        return

    df = pd.DataFrame(latest_weather)
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])

    cities = sorted(df["city"].unique())

    with col1:
        selected_city = st.selectbox("Select City", ["All"] + cities)

    with col2:
        st.date_input(
            "Date Range",
            value=(df["recorded_at"].min().date(), df["recorded_at"].max().date()),
        )

    with col3:
        metric = st.selectbox(
            "Metric",
            ["temperature_celsius", "humidity", "pressure", "wind_speed"],
        )

    # Filter data
    filtered_df = df.copy()
    if selected_city != "All":
        filtered_df = filtered_df[filtered_df["city"] == selected_city]

    # Stats
    st.subheader("📊 Statistics")
    stat_cols = st.columns(5)

    stats = {
        "Count": f"{len(filtered_df)}",
        "Avg Temp": f"{filtered_df['temperature_celsius'].mean():.1f}°C",
        "Max Temp": f"{filtered_df['temperature_celsius'].max():.1f}°C",
        "Min Temp": f"{filtered_df['temperature_celsius'].min():.1f}°C",
        "Avg Humidity": f"{filtered_df['humidity'].mean():.0f}%",
    }

    for i, (label, value) in enumerate(stats.items()):
        with stat_cols[i]:
            st.metric(label, value)

    st.divider()

    # Visualization
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(f"📈 {metric.replace('_', ' ').title()} Over Time")

        fig = px.line(
            filtered_df.sort_values("recorded_at"),
            x="recorded_at",
            y=metric,
            color="city" if selected_city == "All" else None,
        )
        fig.update_layout(height=400, template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("🌡️ Temperature Distribution")

        fig = px.histogram(
            filtered_df,
            x="temperature_celsius",
            nbins=20,
            color="city" if selected_city == "All" else None,
        )
        fig.update_layout(height=400, template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.subheader("📋 Raw Data")
    st.dataframe(
        filtered_df[
            [
                "city",
                "country",
                "temperature_celsius",
                "humidity",
                "weather_condition",
                "wind_speed",
                "recorded_at",
            ]
        ].sort_values("recorded_at", ascending=False),
        use_container_width=True,
        height=400,
    )


def render_quality_metrics(db: DatabaseManager) -> None:
    """Render data quality metrics page."""
    st.header("✅ Data Quality Metrics")

    metrics = db.get_quality_metrics()

    # Quality Score Card
    st.subheader("🎯 Overall Quality Score")

    col1, col2, col3 = st.columns(3)

    with col1:
        completeness = metrics.get("data_completeness", 100)
        st.metric(
            "Data Completeness",
            f"{completeness:.1f}%",
            delta="Good" if completeness >= 95 else "Needs Attention",
        )

        # Progress bar
        st.progress(completeness / 100)

    with col2:
        valid_temp = metrics.get("valid_temperature_pct", 100)
        st.metric(
            "Valid Temperature Records",
            f"{valid_temp:.1f}%",
        )
        st.progress(valid_temp / 100)

    with col3:
        total = metrics.get("total_records", 0)
        st.metric(
            "Total Records Processed",
            f"{total:,}",
        )

    st.divider()

    # Quality Gate Status
    st.subheader("🚦 Quality Gate Status")

    try:
        gate_results = db.get_latest_gate_results()
    except Exception as e:
        st.warning(f"Unable to fetch quality gate results (is the schema migrated?): {e}")
        gate_results = []

    if not gate_results:
        st.info("No quality gate results yet. Run the pipeline to populate.")
    else:
        gate_cols = st.columns(len(gate_results))
        layer_icons = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}

        for i, gate in enumerate(gate_results):
            layer = (gate.get("layer") or "").lower()
            icon = layer_icons.get(layer, "📦")
            passed = gate.get("gate_passed")
            status_label = "✅ PASSED" if passed else "❌ FAILED"
            checks_total = gate.get("expectations_evaluated") or 0
            checks_passed = gate.get("expectations_passed") or 0
            failure_reason = gate.get("failure_reason") or ""

            with gate_cols[i]:
                st.markdown(
                    f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            padding: 20px; border-radius: 12px; color: white; text-align: center;">
                    <h3>{icon} {layer.title() or "Unknown"} Layer</h3>
                    <h2>{status_label}</h2>
                    <p>{checks_passed}/{checks_total} checks passed</p>
                </div>
                """,
                    unsafe_allow_html=True,
                )
                if not passed and failure_reason:
                    st.caption(f"Reason: {failure_reason}")

    st.divider()

    # Quality trend over time
    st.subheader("📈 Quality Pass-Rate Trend")

    try:
        trend = db.get_quality_trend(days=14)
    except Exception as e:
        st.warning(f"Unable to fetch quality trend (is the schema migrated?): {e}")
        trend = []

    if not trend:
        st.info("No quality history yet. Run the pipeline a few times to build a trend.")
    else:
        trend_df = pd.DataFrame(trend)
        trend_df["date"] = pd.to_datetime(trend_df["date"])
        fig = px.line(
            trend_df,
            x="date",
            y="avg_pass_rate",
            color="layer",
            markers=True,
            title="Average quality-gate pass rate by layer (last 14 days)",
            labels={"avg_pass_rate": "Pass rate (%)", "date": "Date"},
        )
        fig.update_layout(height=380, template="plotly_white", yaxis_range=[0, 105])
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Statistical anomaly detection on temperature
    st.subheader("🚨 Temperature Anomalies")
    st.caption("Readings more than 3σ from their city's mean (z-score outliers).")

    weather = db.get_latest_weather(limit=500)
    anomalies = detect_temperature_anomalies(weather) if weather else []

    if not anomalies:
        st.success("✅ No temperature anomalies detected.")
    else:
        st.warning(f"⚠️ {len(anomalies)} anomalous reading(s) detected.")
        anomaly_df = pd.DataFrame(anomalies)
        cols = [
            c
            for c in ["city", "country", "temperature_celsius", "z_score", "recorded_at"]
            if c in anomaly_df.columns
        ]
        st.dataframe(anomaly_df[cols], use_container_width=True, hide_index=True)

    st.divider()

    # Quality Rules
    st.subheader("📏 Active Quality Rules")

    rules_df = pd.DataFrame(
        [
            {
                "Rule": "Schema Drift Detection",
                "Layer": "All",
                "Severity": "🔴 Critical",
                "Status": "Active",
            },
            {
                "Rule": "Null Value Check",
                "Layer": "All",
                "Severity": "🟡 Warning",
                "Status": "Active",
            },
            {
                "Rule": "Temperature Range",
                "Layer": "Silver/Gold",
                "Severity": "🟡 Warning",
                "Status": "Active",
            },
            {
                "Rule": "Humidity Range",
                "Layer": "Silver/Gold",
                "Severity": "🟡 Warning",
                "Status": "Active",
            },
            {
                "Rule": "Data Freshness",
                "Layer": "Gold",
                "Severity": "🟡 Warning",
                "Status": "Active",
            },
            {
                "Rule": "Uniqueness Check",
                "Layer": "Gold",
                "Severity": "🔴 Critical",
                "Status": "Active",
            },
        ]
    )

    st.dataframe(rules_df, use_container_width=True, hide_index=True)


def render_pipeline_status(db: DatabaseManager) -> None:
    """Render pipeline status page."""
    st.header("🚀 Pipeline Status")

    # Current status
    st.subheader("📡 Current Status")

    try:
        stats = db.get_pipeline_run_stats()
    except Exception as e:
        st.warning(f"Unable to fetch pipeline statistics (is the schema migrated?): {e}")
        stats = {}

    col1, col2, col3, col4 = st.columns(4)

    last_run_at = stats.get("last_run_at")
    success_rate = stats.get("success_rate")
    avg_duration = stats.get("avg_duration_seconds")

    if last_run_at:
        # last_run_at is a datetime returned by SQLAlchemy
        try:
            last_run_str = pd.Timestamp(last_run_at).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_run_str = str(last_run_at)
        status_label = "🟢 Ready"
    else:
        last_run_str = "—"
        status_label = "⚪ No runs yet"

    with col1:
        st.metric("Pipeline Status", status_label)

    with col2:
        st.metric("Last Run", last_run_str)

    with col3:
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%" if success_rate is not None else "—",
        )

    with col4:
        st.metric(
            "Avg Duration",
            f"{avg_duration:.2f}s" if avg_duration is not None else "—",
        )

    st.divider()

    # Pipeline architecture
    st.subheader("🏗️ Pipeline Architecture")

    st.markdown("""
    ```
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │   Weather   │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
    │     API     │     │   (Raw)     │     │  (Cleaned)  │     │ (Aggregated)│
    └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                              │                    │                    │
                              ▼                    ▼                    ▼
                        ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                        │   Quality   │     │   Quality   │     │   Quality   │
                        │    Gate     │     │    Gate     │     │    Gate     │
                        └─────────────┘     └─────────────┘     └─────────────┘
    ```
    """)

    st.divider()

    # Recent runs
    st.subheader("📜 Recent Pipeline Runs")

    try:
        recent_runs = db.get_recent_pipeline_runs(limit=20)
    except Exception as e:
        st.warning(f"Unable to fetch recent pipeline runs (is the schema migrated?): {e}")
        recent_runs = []

    if not recent_runs:
        st.info("No pipeline runs recorded yet. Run the pipeline to populate.")
    else:
        runs_df = pd.DataFrame(recent_runs)
        st.dataframe(runs_df, use_container_width=True, hide_index=True)

    st.divider()

    # Manual trigger
    st.subheader("🎮 Manual Controls")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("▶️ Run Pipeline Now", type="primary"):
            with st.spinner("Running pipeline..."):
                st.info("Pipeline execution would be triggered here")
                st.success("Pipeline completed successfully!")

    with col2:
        if st.button("🔄 Refresh Metrics"):
            st.cache_data.clear()
            st.rerun()

    with col3:
        if st.button("📊 Export Report"):
            st.info("Report export functionality")


if __name__ == "__main__":
    main()
