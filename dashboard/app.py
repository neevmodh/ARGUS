"""ARGUS dashboard - serving layer over MongoDB (batch + speed views)."""

import math
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from argus import config
from argus.openmeteo import OpenMeteo
from argus.sample_data import CITIES

st.set_page_config(page_title="ARGUS", page_icon=":eye:", layout="wide")


@st.cache_resource
def mongo():
    return MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=4000)


@st.cache_resource
def weather_client():
    return OpenMeteo(ttl_s=600)


def db():
    return mongo()[config.MONGO_DB]


def frame(cursor) -> pd.DataFrame:
    return pd.DataFrame(list(cursor))


def risk_color(score: float) -> list[int]:
    s = max(0.0, min(100.0, score or 0)) / 100
    return [int(40 + 215 * s), int(190 * (1 - s) + 30), 70, 200]


def us_view(lat=37.5, lng=-96.0, zoom=3.4):
    return pdk.ViewState(latitude=lat, longitude=lng, zoom=zoom, pitch=35)


st.title("ARGUS")
st.caption("Accident Risk Grid & Unified Streaming — Hadoop · MapReduce · Hive · Kafka · Spark · MLlib · MLflow · MongoDB")

try:
    mongo().admin.command("ping")
except PyMongoError as exc:
    st.error(f"MongoDB not reachable ({type(exc).__name__}). Start it with `make up`.")
    st.stop()

tab_live, tab_hot, tab_risk, tab_insight, tab_health = st.tabs(
    ["Live", "Hotspots", "Check my risk", "Insights", "Cluster health"]
)

# ---------------------------------------------------------------- live (speed layer)
with tab_live:

    @st.fragment(run_every="5s")
    def live_panel():
        since = datetime.now(timezone.utc) - timedelta(minutes=5)
        recent = frame(db().live_events.find({"ingested_at": {"$gte": since}}, {"_id": 0})
                       .sort("ingested_at", -1).limit(2000))
        c1, c2, c3, c4 = st.columns(4)
        if recent.empty:
            c1.metric("Events (5 min)", 0)
            st.info("No live events yet. Run `make stream` then `make produce`.")
            return
        c1.metric("Events (5 min)", f"{len(recent):,}")
        c2.metric("Avg risk", f"{recent['risk_score'].mean():.1f}")
        c3.metric("High-risk (≥70)", int((recent["risk_score"] >= 70).sum()))
        c4.metric("States active", recent["state"].nunique())

        recent["lng"] = recent["location"].map(lambda g: g["coordinates"][0])
        recent["lat"] = recent["location"].map(lambda g: g["coordinates"][1])
        recent["color"] = recent["risk_score"].map(risk_color)
        layer = pdk.Layer("ScatterplotLayer", recent.drop(columns=["location", "ingested_at"]),
                          get_position="[lng, lat]", get_fill_color="color",
                          get_radius="400 + risk_score * 40", pickable=True)
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=us_view(),
                                 tooltip={"text": "{city}, {state}\n{street}\nrisk {risk_score} · {weather}"}))

        left, right = st.columns([3, 2])
        windows = frame(db().live_windows.find({"window_start": {"$gte": since - timedelta(minutes=5)}}, {"_id": 0}))
        if not windows.empty:
            by_state = windows.groupby("state", as_index=False).agg(events=("events", "sum"), avg_risk=("avg_risk", "mean"))
            left.altair_chart(
                alt.Chart(by_state.nlargest(15, "events")).mark_bar().encode(
                    x=alt.X("events:Q", title="events (last 10 min, 1-min windows)"),
                    y=alt.Y("state:N", sort="-x"),
                    color=alt.Color("avg_risk:Q", scale=alt.Scale(scheme="orangered"), title="avg risk"),
                ),
                use_container_width=True,
            )
        top = recent.nlargest(10, "risk_score")[["city", "state", "street", "weather", "predicted_severity",
                                                "severity_actual", "risk_score"]]
        right.markdown("**Highest-risk live events**")
        right.dataframe(top, hide_index=True, use_container_width=True)

    live_panel()

# ---------------------------------------------------------------- hotspots (batch layer)
with tab_hot:
    hs = frame(db().hotspots.find({}, {"_id": 0}).sort("risk_index", -1))
    if hs.empty:
        st.info("No hotspots yet. Run `make ml` (K-Means).")
    else:
        hs["lng"] = hs["location"].map(lambda g: g["coordinates"][0])
        hs["lat"] = hs["location"].map(lambda g: g["coordinates"][1])
        hs["color"] = hs["risk_index"].map(lambda r: risk_color(r * 1.0))
        column = pdk.Layer("ColumnLayer", hs.drop(columns=["location"]), get_position="[lng, lat]",
                           get_elevation="accidents", elevation_scale=40, radius=3500,
                           get_fill_color="color", pickable=True, extruded=True)
        st.pydeck_chart(pdk.Deck(layers=[column], initial_view_state=us_view(),
                                 tooltip={"text": "{city}, {state} · {top_street}\n{accidents} accidents\n"
                                                  "avg severity {avg_severity} · risk {risk_index}"}))
        st.dataframe(hs[["risk_index", "city", "state", "top_street", "accidents", "avg_severity",
                         "severe_ratio", "radius_km"]].head(25), hide_index=True, use_container_width=True)

    streets = frame(db().mr_top_streets.find({}, {"_id": 0, "loaded_at": 0}).sort("rank", 1))
    if not streets.empty:
        st.subheader("Most dangerous streets (MapReduce: 2 chained jobs)")
        st.dataframe(streets, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- risk checker
with tab_risk:
    st.markdown("Combines **nearby hotspots** (K-Means), **live weather** (Open-Meteo) × its "
                "**relative risk** (SciPy stats) and the **hour-of-day profile** (MapReduce).")
    presets = {f"{c[0]}, {c[2]}": c for c in CITIES}
    col_a, col_b = st.columns([2, 1])
    choice = col_a.selectbox("Location", ["Custom"] + list(presets))
    if choice == "Custom":
        lat = col_a.number_input("Latitude", value=25.7617, format="%.4f")
        lng = col_a.number_input("Longitude", value=-80.1918, format="%.4f")
        state = col_a.text_input("State (2 letters)", value="FL").upper()
    else:
        _, _, state, lat, lng, _, _ = presets[choice]
    radius_km = col_b.slider("Search radius (km)", 2, 50, 10)
    hour = col_b.slider("Hour of day", 0, 23, datetime.now().hour)

    if st.button("Assess risk", type="primary"):
        near = frame(db().hotspots.aggregate([
            {"$geoNear": {"near": {"type": "Point", "coordinates": [lng, lat]}, "distanceField": "dist_m",
                          "maxDistance": radius_km * 1000, "spherical": True}},
            {"$limit": 10}, {"$project": {"_id": 0}},
        ]))
        proximity = 0.0
        if not near.empty:
            near["dist_km"] = (near["dist_m"] / 1000).round(2)
            proximity = max(r.risk_index * math.exp(-r.dist_km / max(radius_km / 2, 1)) for r in near.itertuples())

        wx = weather_client().current(lat, lng)
        bucket = wx["bucket"] if wx else "Unknown"
        wr = db().weather_risk.find_one({"bucket": bucket}) or {}
        weather_rr = wr.get("relative_risk", 1.0)

        prof = frame(db().mr_state_hour.find({"state": state}, {"_id": 0}))
        hour_factor = 1.0
        if not prof.empty and prof["accidents"].mean() > 0:
            at_hour = prof.loc[prof["hour"] == hour, "accidents"].sum()
            hour_factor = at_hour / prof["accidents"].mean()

        score = min(100.0, max(5.0, proximity) * weather_rr * math.sqrt(max(hour_factor, 0.05)))
        level = "LOW" if score < 25 else "MODERATE" if score < 50 else "HIGH" if score < 75 else "SEVERE"

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("ARGUS risk", f"{score:.0f}/100", level)
        m2.metric("Hotspot proximity", f"{proximity:.1f}", f"{len(near)} hotspots ≤ {radius_km} km")
        m3.metric("Weather now", bucket, f"× {weather_rr:.2f} relative risk")
        m4.metric(f"Hour {hour:02d} factor", f"× {hour_factor:.2f}", state)
        if wx:
            st.caption(f"Open-Meteo: {wx['temperature_f']}°F · visibility {wx['visibility_mi']} mi · "
                       f"wind {wx['wind_speed_mph']} mph · {'day' if wx['is_day'] else 'night'}")
        if not near.empty:
            st.dataframe(near[["dist_km", "city", "top_street", "accidents", "avg_severity", "risk_index"]],
                         hide_index=True, use_container_width=True)
        st.caption("Heuristic composite for demonstration — not a safety guarantee.")

# ---------------------------------------------------------------- insights
with tab_insight:
    models = {m["name"]: m for m in db().models.find({}, {"_id": 0})}
    if "severity_tree" in models:
        m = models["severity_tree"]
        st.subheader("Decision Tree — severity")
        cols = st.columns(4)
        for col, key in zip(cols, ["accuracy", "f1", "recall_sev3", "recall_sev4"], strict=True):
            col.metric(key, f"{m['metrics'].get(key, 0):.3f}")
        fi = pd.DataFrame(m.get("feature_importance", []), columns=["feature", "importance"])
        if not fi.empty:
            st.altair_chart(alt.Chart(fi).mark_bar().encode(x="importance:Q", y=alt.Y("feature:N", sort="-x")),
                            use_container_width=True)

    rules = frame(db().rules.find({}, {"_id": 0, "rule": 1, "lift": 1, "confidence": 1, "support": 1})
                  .sort("lift", -1).limit(15))
    if not rules.empty:
        st.subheader("FP-Growth — conditions that precede severe accidents")
        st.dataframe(rules, hide_index=True, use_container_width=True)

    left, right = st.columns(2)
    wr = frame(db().weather_risk.find({}, {"_id": 0}))
    if not wr.empty:
        left.subheader("Relative risk of a severe accident by weather")
        left.altair_chart(alt.Chart(wr).mark_bar().encode(
            x=alt.X("relative_risk:Q"), y=alt.Y("bucket:N", sort="-x"),
            tooltip=["bucket", "n", "severe_ratio", "relative_risk"]), use_container_width=True)
    fc = frame(db().forecasts.find({}, {"_id": 0}).sort("date", -1).limit(25))
    if not fc.empty:
        right.subheader("Linear Regression — next-day forecast")
        right.dataframe(fc[["place", "date", "predicted_accidents", "trailing_7d_mean"]],
                        hide_index=True, use_container_width=True)

    sh = frame(db().mr_state_hour.find({}, {"_id": 0}))
    if not sh.empty:
        st.subheader("Accidents by state × hour (MapReduce)")
        top_states = sh.groupby("state")["accidents"].sum().nlargest(20).index
        st.altair_chart(alt.Chart(sh[sh["state"].isin(top_states)]).mark_rect().encode(
            x=alt.X("hour:O"), y=alt.Y("state:N"), color=alt.Color("accidents:Q", scale=alt.Scale(scheme="reds"))),
            use_container_width=True)

    tests = frame(db().stats_tests.find({}, {"_id": 0, "created_at": 0, "mlflow_run_id": 0}))
    if not tests.empty:
        st.subheader("Hypothesis tests (SciPy)")
        st.dataframe(tests, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- health (fault-tolerance demos)
with tab_health:

    @st.fragment(run_every="3s")
    def health_panel():
        left, right = st.columns(2)
        left.subheader("MongoDB replica set (CAP demo)")
        try:
            status = mongo().admin.command("replSetGetStatus")
            members = pd.DataFrame([{"member": m["name"], "state": m["stateStr"], "healthy": bool(m["health"]),
                                     "last_heartbeat": m.get("lastHeartbeat")} for m in status["members"]])
            primary = members.loc[members["state"] == "PRIMARY", "member"]
            left.metric("Primary", primary.iloc[0] if len(primary) else "— electing —")
            left.dataframe(members, hide_index=True, use_container_width=True)
        except PyMongoError as exc:
            left.error(f"replSetGetStatus failed: {type(exc).__name__} (election in progress?)")

        right.subheader("HDFS (fault-tolerance demo)")
        try:
            base = f"{config.NAMENODE_HTTP}/jmx?qry=Hadoop:service=NameNode,name="
            state = requests.get(base + "FSNamesystemState", timeout=3).json()["beans"][0]
            fsn = requests.get(base + "FSNamesystem", timeout=3).json()["beans"][0]
            c1, c2, c3 = right.columns(3)
            c1.metric("Live DataNodes", state["NumLiveDataNodes"])
            c2.metric("Dead DataNodes", state["NumDeadDataNodes"])
            c3.metric("Under-replicated blocks", fsn["UnderReplicatedBlocks"])
            right.caption(f"Blocks: {fsn['BlocksTotal']:,} · missing: {fsn['MissingBlocks']} · "
                          f"used {state['CapacityUsed'] / 1e9:.2f} GB of {state['CapacityTotal'] / 1e9:.1f} GB")
        except (requests.RequestException, KeyError, IndexError, ValueError) as exc:
            right.error(f"NameNode JMX unavailable: {type(exc).__name__}")

    health_panel()
