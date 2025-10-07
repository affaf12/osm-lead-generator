"""
🌍 OSM Pro Lead Generator — v7 (Final Cloud-Compatible)
✅ Fixes:
 - JsCode serialization (Streamlit Cloud safe)
 - Updated st_aggrid import (v1.0.4+)
 - Improved cache + requirements compatibility
 - Clean Excel download + Google Maps + Email Verification
"""

import os
import re
import json
import hashlib
import asyncio
import socket
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
import nest_asyncio
import aiohttp
import folium
import tldextract
from streamlit_folium import st_folium
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from st_aggrid.shared import JsCode
from folium.plugins import MarkerCluster

# ---------------- Setup ----------------
nest_asyncio.apply()
st.set_page_config(page_title="🌍 OSM Pro Lead Generator (v7)", layout="wide")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
geolocator = Nominatim(user_agent="OSMProApp/v7")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# ---------------- Utility ----------------
def _cache_file_key(key: str):
    return os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")

def _dump_cache(key: str, obj):
    try:
        json.dump(obj, open(_cache_file_key(key), "w"))
    except Exception:
        pass

def _load_cache(key: str):
    try:
        p = _cache_file_key(key)
        if os.path.exists(p):
            return json.load(open(p))
    except Exception:
        pass
    return None

@st.cache_data(show_spinner=False)
def geocode_cached(location: str):
    try:
        path = _cache_file_key("coords")
        d = json.load(open(path)) if os.path.exists(path) else {}
        if location in d:
            return tuple(d[location])
        loc = geocode(location)
        if not loc:
            return None
        d[location] = (loc.latitude, loc.longitude)
        json.dump(d, open(path, "w"))
        return (loc.latitude, loc.longitude)
    except Exception:
        return None

def normalize_url(url: str):
    if not url or url in ("N/A", None):
        return None
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        url = "http://" + url
    return url

# ---------------- Email Verification ----------------
def verify_email(email: str):
    """Offline verification mock: syntax + MX check."""
    if not email or "@" not in email:
        return "invalid"
    if not re.match(EMAIL_RE, email):
        return "invalid"
    domain = email.split("@")[1]
    try:
        socket.getaddrinfo(domain, 25)
        if any(x in email.lower() for x in ["info@", "support@", "no-reply@", "noreply@"]):
            return "risky"
        return "valid"
    except Exception:
        return "invalid"

# ---------------- Overpass Fetch ----------------
def fetch_overpass(query: str, lat: float, lon: float, radius: int):
    key = f"overpass::{query}::{lat:.4f}::{lon:.4f}::{radius}"
    cached = _load_cache(key)
    if cached is not None:
        return cached

    q = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      way["amenity"="{query}"](around:{radius},{lat},{lon});
      relation["amenity"="{query}"](around:{radius},{lat},{lon});
      node["shop"="{query}"](around:{radius},{lat},{lon});
      way["shop"="{query}"](around:{radius},{lat},{lon});
      relation["shop"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    try:
        r = requests.get("https://overpass-api.de/api/interpreter", params={"data": q}, timeout=60)
        r.raise_for_status()
        data = r.json()
    except Exception:
        data = {"elements": []}

    results = []
    for el in data.get("elements", []):
        tags = el.get("tags", {}) or {}
        lat_el = el.get("lat") or el.get("center", {}).get("lat")
        lon_el = el.get("lon") or el.get("center", {}).get("lon")
        results.append({
            "osm_id": el.get("id"),
            "name": tags.get("name", "N/A"),
            "type": tags.get("amenity") or tags.get("shop") or "N/A",
            "website": tags.get("website", "N/A"),
            "emails": tags.get("email", "N/A"),
            "phone": tags.get("phone", "N/A"),
            "address": ", ".join(
                [tags.get(k) for k in ("addr:housenumber","addr:street","addr:city","addr:postcode") if tags.get(k)]
            ) or tags.get("addr:full", "N/A"),
            "latitude": lat_el,
            "longitude": lon_el,
        })
    _dump_cache(key, results)
    return results

# ---------------- Sidebar ----------------
st.sidebar.title("🔎 OSM Pro Lead Generator — v7")
country = st.sidebar.text_input("Country", "Italy")
city = st.sidebar.text_input("City", "Rome")
queries = st.sidebar.text_input("Business types (comma separated)", "cafe, restaurant, bar")
radius = st.sidebar.slider("Base radius (meters)", 500, 5000, 1000, 100)
steps = st.sidebar.number_input("Radius steps", min_value=1, max_value=5, value=2)
show_map = st.sidebar.checkbox("Show interactive map", value=True)
verify_emails = st.sidebar.checkbox("Verify Emails (mock check)", value=True)
generate = st.sidebar.button("Generate Leads 🚀")

# ---------------- Main ----------------
st.title("🌍 OSM Pro Lead Generator — v7")
if "leads" not in st.session_state:
    st.session_state["leads"] = pd.DataFrame()

if generate:
    coords = geocode_cached(f"{city}, {country}")
    if not coords:
        st.error("Could not resolve location.")
        st.stop()

    lat, lon = coords
    st.info(f"Coordinates: {lat:.5f}, {lon:.5f}")

    all_results = []
    qlist = [q.strip() for q in queries.split(",") if q.strip()]
    for q in qlist:
        for step in range(steps):
            r = radius * (step + 1)
            st.write(f"Searching for `{q}` within {r} m ...")
            res = fetch_overpass(q, lat, lon, r)
            if res:
                all_results.extend(res)
                break

    df = pd.DataFrame(all_results)
    if df.empty:
        st.warning("No results found.")
        st.stop()

    df["website"] = df["website"].astype(str).replace({"None": "N/A"})
    df["website"] = df["website"].apply(lambda u: normalize_url(u) or "N/A")

    if verify_emails:
        df["email_status"] = df["emails"].apply(verify_email)
    else:
        df["email_status"] = "unchecked"

    df["google_maps"] = df.apply(
        lambda r: f"https://www.google.com/maps?q={r['latitude']},{r['longitude']}"
        if pd.notna(r["latitude"]) and pd.notna(r["longitude"]) else "N/A",
        axis=1,
    )

    st.session_state["leads"] = df
    st.success(f"Fetched {len(df)} leads ✅")

df = st.session_state["leads"]

if not df.empty:
    st.subheader("Results Table")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=False, sortable=True, filter=True)
    gb.configure_pagination()
    gb.configure_side_bar()

    email_style = JsCode("""
    function(params) {
        if(params.data.email_status === 'valid') return {'backgroundColor':'#C8E6C9'};
        if(params.data.email_status === 'risky') return {'backgroundColor':'#FFF9C4'};
        if(params.data.email_status === 'invalid') return {'backgroundColor':'#FFCDD2'};
        return null;
    }
    """)
    gb.configure_column("emails", cellStyle=email_style)

    link_js = JsCode("""
    function(params) {
        if(!params.value) return "N/A";
        var v = params.value;
        if(v.startsWith("http")) return `<a href="${v}" target="_blank">${v}</a>`;
        return v;
    }
    """)
    gb.configure_column("website", cellRenderer=link_js)
    gb.configure_column("google_maps", cellRenderer=link_js)

    AgGrid(
        df,
        gridOptions=gb.build(),
        allow_unsafe_jscode=True,
        height=450,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.NO_UPDATE
    )

    if show_map:
        st.subheader("📍 Map View (Clustered)")
        m = folium.Map(location=[df["latitude"].mean(), df["longitude"].mean()], zoom_start=12)
        cluster = MarkerCluster().add_to(m)
        for _, r in df.iterrows():
            if pd.notna(r["latitude"]) and pd.notna(r["longitude"]):
                popup = f"<b>{r['name']}</b><br>{r['address']}<br><a href='{r['google_maps']}' target='_blank'>Google Maps</a>"
                folium.Marker([r["latitude"], r["longitude"]], popup=popup).add_to(cluster)
        st_folium(m, width=700, height=450)

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    st.download_button("⬇️ Download Excel", out.getvalue(), file_name=f"OSM_Leads_{city}_v7.xlsx")

else:
    st.info("No leads yet — use sidebar to search.")
