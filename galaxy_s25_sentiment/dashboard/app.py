import io
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from pandas.api.types import is_bool_dtype, is_numeric_dtype

# -----------------------
# Configuration
# -----------------------
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "streamlit-dashboard")
MAX_HISTORY = 500  # retain last N records in memory
MAX_FETCH = 200    # max records to poll per refresh
DEFAULT_REFRESH_SECONDS = 5

st.set_page_config(page_title="Sentiment Dashboard", layout="wide")
st.title("Galaxy S25 Ultra Sentiment Dashboard")
st.caption("Monitor live streaming data from Kafka or explore your own datasets with interactive visualizations.")

# -----------------------
# Sidebar Navigation
# -----------------------
st.sidebar.header("Navigation")
view_mode = st.sidebar.radio(
    "Select dashboard mode",
    options=("Live Stream", "Upload & Analyze"),
    index=0,
)


# -----------------------
# Kafka Utilities
# -----------------------
@st.cache_resource(show_spinner=False)
def get_consumer(topic: str, bootstrap_servers: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=MAX_FETCH,
    )


def fetch_messages(kafka_consumer: KafkaConsumer) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    try:
        polled = kafka_consumer.poll(timeout_ms=500, max_records=MAX_FETCH)
        for partition_records in polled.values():
            for message in partition_records:
                records.append(message.value)
    except Exception as exc:  # pragma: no cover - debugging aid
        st.error(f"Error reading from Kafka: {exc}")
    return records


def trigger_rerun() -> None:
    rerun_callable = getattr(st, "experimental_rerun", None) or getattr(st, "rerun", None)
    if rerun_callable:
        rerun_callable()
    else:
        st.sidebar.warning(
            "Auto-refresh not supported in this Streamlit version. Click 'Rerun' to refresh manually."
        )


# -----------------------
# Dataset Upload Utilities
# -----------------------
@st.cache_data(show_spinner=False)
def load_uploaded_data(file_bytes: bytes, filename: str, mime_type: str) -> pd.DataFrame:
    """Load user-uploaded dataset into a DataFrame."""
    buffer = io.BytesIO(file_bytes)
    name = filename.lower()

    if name.endswith(".csv"):
        buffer.seek(0)
        data = pd.read_csv(buffer)
    elif name.endswith((".xlsx", ".xls")):
        buffer.seek(0)
        data = pd.read_excel(buffer)
    elif name.endswith(".json"):
        buffer.seek(0)
        try:
            data = pd.read_json(buffer)
        except ValueError:
            buffer.seek(0)
            data = pd.read_json(buffer, lines=True)
    else:
        raise ValueError("Unsupported file format. Please upload CSV, Excel, or JSON files.")

    if data.empty:
        raise ValueError("Uploaded file contains no data.")

    data.columns = [str(col) for col in data.columns]
    return data


# -----------------------
# Live Stream View
# -----------------------
def simulate_stream(seed: int | None, points: int) -> pd.DataFrame:
    """Generate synthetic streaming data with trend, categories, and proportions."""
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range(end=datetime.utcnow(), periods=points, freq="s")
    base = np.linspace(40, 60, points)
    noise = rng.normal(0, 5, points)
    value = base + noise
    categories = rng.choice(["positive", "neutral", "negative"], size=points, p=[0.5, 0.3, 0.2])
    proportions = rng.dirichlet(alpha=[2, 1.5, 1], size=points)
    proportion_df = pd.DataFrame(proportions, columns=["feature_a", "feature_b", "feature_c"])
    return pd.DataFrame({
        "timestamp": timestamps,
        "value": value,
        "category": categories,
    }).join(proportion_df)


if view_mode == "Live Stream":
    st.sidebar.header("Live Stream Controls")
    mode = st.sidebar.selectbox(
        "Data source",
        options=("Simulated", "Kafka"),
        help="Use built-in simulated data or consume from Kafka.",
    )
    refresh_interval = st.sidebar.slider(
        "Auto-refresh interval (seconds)",
        min_value=1,
        max_value=30,
        value=DEFAULT_REFRESH_SECONDS,
        step=1,
    )
    auto_refresh_enabled = st.sidebar.checkbox("Enable auto-refresh", value=True)
    max_points = st.sidebar.slider(
        "Maximum points to retain",
        min_value=50,
        max_value=2000,
        value=500,
        step=50,
        help="Limit the number of records kept in memory for plotting.",
    )
    seed_value = st.sidebar.number_input(
        "Simulation seed (optional)",
        min_value=0,
        max_value=9999,
        value=0,
        help="Use a fixed seed for reproducible simulated data.",
    )
    start_stop = st.sidebar.toggle("Start streaming", value=True, help="Pause or resume automatic updates.")

    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    if "last_refresh_ts" not in st.session_state:
        st.session_state["last_refresh_ts"] = None

    manual_refresh = st.sidebar.button(
        "Refresh once",
        help="Fetch the latest batch immediately when auto-refresh is paused.",
    )
    should_update = start_stop or manual_refresh or not st.session_state["messages"]

    data_placeholder = st.empty()
    metrics_placeholder = st.empty()
    charts_placeholder = st.container()

    if mode == "Kafka":
        consumer = get_consumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP, GROUP_ID)

        if "initialized" not in st.session_state:
            st.session_state["initialized"] = False

        # Ensure we read the backlog only once per session
        if not st.session_state["initialized"]:
            assignment_attempts = 0
            while not consumer.assignment() and assignment_attempts < 5:
                consumer.poll(timeout_ms=200)
                assignment_attempts += 1

            if consumer.assignment():
                consumer.seek_to_beginning()
            st.session_state["initialized"] = True

        if should_update:
            new_messages = fetch_messages(consumer)
            if new_messages:
                st.session_state["messages"].extend(new_messages)
                st.session_state["messages"] = st.session_state["messages"][-max_points:]
    else:
        if should_update:
            points = max(refresh_interval * 5, 1)
            seed = seed_value if seed_value != 0 else None
            simulated_df = simulate_stream(seed=seed, points=points)
            st.session_state["messages"].extend(simulated_df.to_dict(orient="records"))
            st.session_state["messages"] = st.session_state["messages"][-max_points:]

    if should_update:
        st.session_state["last_refresh_ts"] = datetime.now()

    all_messages = st.session_state["messages"]

    if all_messages:
        df_stream = pd.DataFrame(all_messages)
        if "timestamp" in df_stream.columns:
            df_stream["timestamp"] = pd.to_datetime(df_stream["timestamp"], errors="coerce")
            df_stream.sort_values("timestamp", inplace=True)

        if "value" in df_stream.columns:
            df_stream["value_numeric"] = pd.to_numeric(df_stream["value"], errors="coerce")

        st.sidebar.metric("Cached messages", len(all_messages))
        if st.session_state["last_refresh_ts"] is not None:
            sidebar_caption = st.session_state["last_refresh_ts"].strftime("%Y-%m-%d %H:%M:%S")
        else:
            sidebar_caption = "pending"
        st.sidebar.caption(f"Last refresh: {sidebar_caption}")

        with data_placeholder:
            st.subheader("Latest Records")
            st.dataframe(df_stream.tail(50), use_container_width=True)

        value_series = df_stream.get("value_numeric")
        if value_series is not None and not value_series.dropna().empty:
            avg_value = float(value_series.mean())
            min_value = float(value_series.min())
            max_value = float(value_series.max())
        else:
            avg_value = min_value = max_value = float("nan")

        with metrics_placeholder:
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            metric_col1.metric("Average value", f"{avg_value:.2f}" if not np.isnan(avg_value) else "–")
            metric_col2.metric("Minimum value", f"{min_value:.2f}" if not np.isnan(min_value) else "–")
            metric_col3.metric("Maximum value", f"{max_value:.2f}" if not np.isnan(max_value) else "–")

        with charts_placeholder:
            st.subheader("Live Visualizations")

            chart_tabs = st.tabs(["Line", "Bar", "Pie"])

            with chart_tabs[0]:
                if "timestamp" in df_stream.columns and "value_numeric" in df_stream.columns:
                    line_fig = px.line(
                        df_stream.tail(max_points),
                        x="timestamp",
                        y="value_numeric",
                        title="Value over time",
                        markers=True,
                    )
                    line_fig.update_layout(margin=dict(t=40, b=40, l=40, r=20))
                    st.plotly_chart(line_fig, use_container_width=True)
                else:
                    st.info("Timestamp or value fields missing for line chart.")

            with chart_tabs[1]:
                category_field = "category" if "category" in df_stream.columns else None
                if category_field:
                    category_counts = df_stream[category_field].fillna("Unknown").value_counts().reset_index()
                    category_counts.columns = ["category", "count"]
                    bar_fig = px.bar(
                        category_counts,
                        x="category",
                        y="count",
                        color="category",
                        title="Category frequency",
                        text="count",
                    )
                    bar_fig.update_layout(margin=dict(t=40, b=40, l=40, r=20))
                    st.plotly_chart(bar_fig, use_container_width=True)
                else:
                    st.info("No categorical field available for bar chart.")

            with chart_tabs[2]:
                value_columns = [col for col in df_stream.columns if col.startswith("feature_")]
                if value_columns:
                    latest_row = df_stream[value_columns].dropna().tail(1)
                    if not latest_row.empty:
                        latest_values = latest_row.iloc[0]
                        pie_fig = px.pie(
                            values=latest_values.values,
                            names=value_columns,
                            title="Latest proportional metrics",
                        )
                        st.plotly_chart(pie_fig, use_container_width=True)
                    else:
                        st.info("Waiting for non-null proportion values.")
                else:
                    st.info("No proportion fields available for pie chart.")
    else:
        st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        st.warning("No data available yet. Configure your data source to start streaming.")

    if auto_refresh_enabled and start_stop:
        time.sleep(refresh_interval)
        trigger_rerun()


# -----------------------
# Upload & Analyze View
# -----------------------
if view_mode == "Upload & Analyze":
    st.sidebar.header("Upload Controls")
    st.sidebar.markdown(
        "Upload a dataset (CSV, Excel, or JSON) to explore interactive sentiment visualizations."
    )
    uploaded_file = st.sidebar.file_uploader(
        "Upload dataset",
        type=["csv", "xlsx", "xls", "json"],
        accept_multiple_files=False,
    )

    if uploaded_file is None:
        st.info("Upload a dataset to explore the interactive dashboard.")
    else:
        try:
            dataset = load_uploaded_data(
                uploaded_file.getvalue(), uploaded_file.name, uploaded_file.type
            )
        except ValueError as exc:
            st.error(f"Could not load dataset: {exc}")
        else:
            row_count, col_count = dataset.shape
            st.success(
                f"Loaded **{row_count:,}** rows and **{col_count:,}** columns from `{uploaded_file.name}`."
            )

            with st.expander("Preview data (first 100 rows)", expanded=False):
                st.dataframe(dataset.head(100), use_container_width=True)

            categorical_candidates = [
                col
                for col in dataset.columns
                if dataset[col].dtype == "object"
                or dataset[col].dtype.name == "category"
                or is_bool_dtype(dataset[col])
                or dataset[col].nunique(dropna=True) <= 25
            ]
            numeric_candidates = [
                col for col in dataset.columns if is_numeric_dtype(dataset[col])
            ]

            st.divider()
            st.subheader("Visualization Controls")
            controls_col1, controls_col2 = st.columns(2)

            with controls_col1:
                if categorical_candidates:
                    category_column = st.selectbox(
                        "Select categorical column",
                        options=categorical_candidates,
                        help="Choose a column representing sentiment or categories.",
                    )
                else:
                    category_column = None
                    st.warning(
                        "No categorical columns detected. Add a categorical column to enable bar and pie charts."
                    )

            with controls_col2:
                if numeric_candidates:
                    numeric_column = st.selectbox(
                        "Select numeric column",
                        options=numeric_candidates,
                        help="Choose a numeric column for histogram analysis (e.g., score or rating).",
                    )
                else:
                    numeric_column = None
                    st.warning(
                        "No numeric columns detected. Add a numeric column to enable histograms."
                    )

            filtered_df = dataset.copy()
            category_labels = None

            if category_column:
                category_labels = filtered_df[category_column].fillna("Unknown").astype(str)
                filtered_df["_category_label"] = category_labels
                available_categories = sorted(filtered_df["_category_label"].unique())

                default_selection = available_categories
                selected_categories = st.multiselect(
                    f"Filter categories in `{category_column}`",
                    options=available_categories,
                    default=default_selection,
                    help="Select one or more categories to focus the analysis.",
                )

                if selected_categories:
                    filtered_df = filtered_df[
                        filtered_df["_category_label"].isin(selected_categories)
                    ]
                else:
                    filtered_df = filtered_df.iloc[0:0]
                    st.info("Select at least one category to display visualizations.")
            else:
                filtered_df["_category_label"] = "All"
                selected_categories = ["All"]

            numeric_range = None
            if numeric_column:
                filtered_df["_numeric_value"] = pd.to_numeric(
                    filtered_df[numeric_column], errors="coerce"
                )
                numeric_series = filtered_df["_numeric_value"].dropna()
                if numeric_series.empty:
                    st.info(
                        "No numeric values available after filtering. Histogram will be skipped."
                    )
                else:
                    min_val = float(numeric_series.min())
                    max_val = float(numeric_series.max())
                    if min_val == max_val:
                        numeric_range = (min_val, max_val)
                        st.info(
                            f"Column `{numeric_column}` contains a single value ({min_val}). Histogram will show a single bin."
                        )
                    else:
                        numeric_range = st.slider(
                            f"Value range for `{numeric_column}`",
                            min_value=float(min_val),
                            max_value=float(max_val),
                            value=(float(min_val), float(max_val)),
                            help="Drag the handles to narrow the numeric range included in the charts.",
                        )

                    if numeric_range:
                        lower, upper = numeric_range
                        filtered_df = filtered_df[
                            filtered_df["_numeric_value"].between(lower, upper, inclusive="both")
                            | filtered_df["_numeric_value"].isna()
                        ]

            st.metric(
                "Visible records",
                f"{len(filtered_df):,}",
                help="Number of records that match the current filters.",
            )

            if filtered_df.empty:
                st.warning("No data available for the selected filters. Adjust your selections to see visualizations.")
            else:
                st.subheader("Category Insights")
                counts_df = (
                    filtered_df["_category_label"].value_counts().rename_axis("category").reset_index(name="count")
                )
                counts_df["percentage"] = counts_df["count"] / counts_df["count"].sum() * 100

                charts_col1, charts_col2 = st.columns(2)

                with charts_col1:
                    fig_bar = px.bar(
                        counts_df,
                        x="category",
                        y="count",
                        color="category",
                        text="count",
                        custom_data=["percentage"],
                        labels={"category": category_column or "Category", "count": "Count"},
                    )
                    fig_bar.update_layout(
                        title="Count by Category",
                        xaxis_title=category_column or "Category",
                        yaxis_title="Count",
                        showlegend=False,
                        hovermode="closest",
                        margin=dict(t=60, b=40, l=40, r=20),
                    )
                    fig_bar.update_traces(
                        texttemplate="%{text}",
                        textposition="outside",
                        hovertemplate="<b>%{x}</b><br>Count: %{y}<br>Percentage: %{customdata[0]:.2f}%<extra></extra>",
                    )
                    st.plotly_chart(fig_bar, use_container_width=True, theme="streamlit")

                with charts_col2:
                    fig_pie = px.pie(
                        counts_df,
                        names="category",
                        values="count",
                        hole=0.3,
                        labels={"category": category_column or "Category", "count": "Count"},
                    )
                    fig_pie.update_traces(
                        textinfo="percent+label",
                        hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>",
                    )
                    fig_pie.update_layout(
                        title="Category Distribution",
                        margin=dict(t=60, b=40, l=20, r=20),
                    )
                    st.plotly_chart(fig_pie, use_container_width=True, theme="streamlit")

                st.subheader("Numeric Distribution")
                if numeric_column and "_numeric_value" in filtered_df.columns:
                    numeric_series_filtered = filtered_df["_numeric_value"].dropna()
                    if numeric_series_filtered.empty:
                        st.info("No numeric data available for the histogram after applying filters.")
                    else:
                        bins = st.slider(
                            "Number of histogram bins",
                            min_value=5,
                            max_value=60,
                            value=20,
                            step=1,
                            help="Adjust the number of bins to change the histogram granularity.",
                        )
                        fig_hist = px.histogram(
                            filtered_df,
                            x="_numeric_value",
                            color="_category_label" if category_column else None,
                            nbins=bins,
                            labels={
                                "_numeric_value": numeric_column or "Value",
                                "_category_label": category_column or "Category",
                            },
                        )
                        fig_hist.update_traces(
                            hovertemplate="<b>%{x}</b><br>Count: %{y}<extra></extra>",
                        )
                        fig_hist.update_layout(
                            title=f"{numeric_column or 'Value'} Distribution",
                            xaxis_title=numeric_column or "Value",
                            yaxis_title="Frequency",
                            legend_title=category_column or "Category",
                            bargap=0.05,
                            margin=dict(t=60, b=40, l=40, r=20),
                        )
                        st.plotly_chart(fig_hist, use_container_width=True, theme="streamlit")
                else:
                    st.info("Select a numeric column to visualize its distribution.")

            # Clean auxiliary columns from the in-memory DataFrame to avoid confusion in caching
            if "_category_label" in filtered_df.columns:
                filtered_df.drop(columns=["_category_label"], inplace=True)
            if "_numeric_value" in filtered_df.columns:
                filtered_df.drop(columns=["_numeric_value"], inplace=True)