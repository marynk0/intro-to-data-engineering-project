import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime
import plotly.express as px

# -----------------------------
# CONNECT TO CASSANDRA
# -----------------------------
@st.cache_resource
def get_session():
    cluster = Cluster(['127.0.0.1'])  # Update if your Cassandra IP is different
    session = cluster.connect('logistics')  # Your keyspace
    return session

# -----------------------------
# LOAD DATA
# -----------------------------
@st.cache_data
def load_data():
    session = get_session()
    query = """
        SELECT branch, route, delivery_id, arrival_date, beneficiaries_count,
               capacity_utilisation, cargo_subtype, cargo_type, created_date,
               day_of_week, distribution_center, driver_name, region,
               released_tonnes, requested_tonnes, return_percentage,
               status, urgency_level, vehicle, vehicle_load_tonnes,
               warehouse_release_time_hours, week_range
        FROM humanitarian_deliveries;
    """
    rows = session.execute(query)
    df = pd.DataFrame(rows)
    # Convert datetime columns
    if "arrival_date" in df.columns:
        df["arrival_date"] = pd.to_datetime(df["arrival_date"])
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"])
    return df


# STREAMLIT UI

st.title("🚚 Humanitarian Deliveries Dashboard (Cassandra)")

df = load_data()

# Sidebar filters
st.sidebar.header("Filters")
regions = df["region"].dropna().unique()
selected_region = st.sidebar.selectbox("Region", regions)

status = st.sidebar.multiselect(
    "Delivery Status",
    df["status"].unique(),
    default=df["status"].unique()
)

weeks = sorted(df["week_range"].dropna().unique())

selected_week = st.sidebar.selectbox(
    "Week Range",
    options=weeks
)

filtered = df[
    (df["region"] == selected_region) &
    (df["status"].isin(status)) &
    (df["week_range"] == selected_week)
]

# KPIs

st.subheader("📌 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Deliveries", len(filtered))
col2.metric("Total Beneficiaries", int(filtered["beneficiaries_count"].sum()))
col3.metric("Avg Capacity Utilisation", f"{filtered['capacity_utilisation'].mean():.1f}%")
col4.metric("Avg Return %", f"{filtered['return_percentage'].mean():.1f}%")



st.subheader("🚚 Total Trips per Branch")
trips_df = filtered.groupby("branch").size().reset_index(name="trips")
fig_trips = px.bar(trips_df, x="branch", y="trips", text="trips",
                   title="Total Trips per Branch")
st.plotly_chart(fig_trips, use_container_width=True)


# trips per status per branch
st.subheader("📊 Trips Status per Branch")
status_branch_df = filtered.groupby(["branch", "status"]).size().reset_index(name="count")
fig_status_branch = px.bar(status_branch_df, x="branch", y="count", color="status",
                           title="Delivery Status per Branch", barmode="group")
st.plotly_chart(fig_status_branch, use_container_width=True)


# Time Series Plot

st.subheader("📈 Deliveries Over Time")
if not filtered.empty:
    time_df = filtered.groupby("arrival_date").size().reset_index(name="deliveries")
    fig_time = px.line(time_df, x="arrival_date", y="deliveries", markers=True,
                       title="Deliveries Over Time")
    st.plotly_chart(fig_time, use_container_width=True)




# -----------------------------
# Week-wise Deliveries Bar Chart
# -----------------------------
st.subheader("📊 Deliveries by Week Range")
week_df = filtered.groupby("week_range").size().reset_index(name="deliveries")
fig_week = px.bar(week_df, x="week_range", y="deliveries", text="deliveries",
                  title="Total Deliveries per Week")
st.plotly_chart(fig_week, use_container_width=True)

#average capacity utilization
st.subheader("📈 Average Capacity Utilisation per Branch")
capacity_branch_df = filtered.groupby("branch")["capacity_utilisation"].mean().reset_index()
fig_capacity_branch = px.bar(capacity_branch_df, x="branch", y="capacity_utilisation",
                             title="Average Capacity Utilisation (%) per Branch")
st.plotly_chart(fig_capacity_branch, use_container_width=True)

#trips per vehicle
st.subheader("🚛 Trips per Vehicle")
vehicle_df = filtered["vehicle"].value_counts().reset_index()
vehicle_df.columns = ["vehicle", "trips"]
fig_vehicle = px.pie(vehicle_df, names="vehicle", values="trips", title="Trips per Vehicle Type")
st.plotly_chart(fig_vehicle, use_container_width=True)

# Vehicle Load Distribution
# -----------------------------
st.subheader("🚛 Vehicle Load Utilisation")
fig_load = px.histogram(filtered, x="vehicle_load_tonnes", nbins=20,
                        title="Vehicle Load Distribution (tonnes)")
st.plotly_chart(fig_load, use_container_width=True)

# -----------------------------
# Urgency Level Pie Chart
# -----------------------------
st.subheader("⚠️ Delivery Urgency Distribution")
urgency_df = filtered["urgency_level"].value_counts().reset_index()
urgency_df.columns = ["urgency_level", "count"]
fig_urgency = px.pie(urgency_df, names="urgency_level", values="count",
                     title="Deliveries by Urgency Level")
st.plotly_chart(fig_urgency, use_container_width=True)

# -----------------------------
# Cargo Type Breakdown by Region
# -----------------------------
st.subheader("📦 Cargo Type Distribution by Region")
cargo_df = filtered.groupby(["cargo_type", "region"]).size().reset_index(name="count")
fig_cargo = px.bar(cargo_df, x="region", y="count", color="cargo_type",
                   title="Cargo Type per Region", barmode="stack")
st.plotly_chart(fig_cargo, use_container_width=True)



#cargo, stacked bar
st.subheader("📦 Cargo Type per Branch")
cargo_branch_df = filtered.groupby(["branch", "cargo_type"]).size().reset_index(name="count")
fig_cargo_branch = px.bar(cargo_branch_df, x="branch", y="count", color="cargo_type",
                          title="Cargo Type Distribution per Branch", barmode="stack")
st.plotly_chart(fig_cargo_branch, use_container_width=True)


#cargo subtype
st.subheader("🛠 Cargo Subtype per Branch")
subcargo_df = filtered.groupby(["branch", "cargo_subtype"]).size().reset_index(name="count")
fig_subcargo = px.bar(subcargo_df, x="branch", y="count", color="cargo_subtype",
                      title="Cargo Subtype per Branch", barmode="stack")
st.plotly_chart(fig_subcargo, use_container_width=True)


# -----------------------------
# Highlight Delayed or Under-capacity Deliveries
# -----------------------------
st.subheader("⏱ Delayed or Low Utilisation Deliveries")
low_capacity = filtered[(filtered["capacity_utilisation"] < 50) | (filtered["status"] != "Delivered")]
st.dataframe(low_capacity)

# -----------------------------
# Table View & Download
# -----------------------------
st.subheader("📋 Detailed Deliveries Data")
st.dataframe(filtered)

st.download_button(
    "Download Filtered Data as CSV",
    filtered.to_csv(index=False),
    "deliveries_filtered.csv",
    "text/csv"
)
