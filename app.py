"""
🌍 OSM Pro Lead Generator — v9 (Streamlit Cloud Optimized)
✅ Cloud-safe version with caching, error handling & fast load
"""

import os
import re
import json
import hashlib
from io import BytesIO
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
import folium
import tldextract
from streamlit_folium import st_folium
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode
from folium.plugins import MarkerCluster

# ---------------- Page Setup ----------------
st.set_page_config(page_title="🌍 OSM Pro Lead Generator (v9)", layout="wide")
st.title("🌍 OSM Pro Lead Generator — Cloud Optimized v9")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

geolocator = Nominatim(user_agent="OSMProApp/v9")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# ---------------- Utilities ----------------
@st.cache_data(show_spinner=False)
def geocode_city(city, country):
    """Convert city+country to coordinates."""
    loc = geocode(f"{city}, {country}")
    if loc:
        return (loc.latitude, loc.longitude)
    return None

def normalize_url(url):
    if not url or url in ("N/A", None):
        return None
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        url = "http://" + url
    return url

@st.cache_data(show_spinner=False)
def fetch_osm_data(query, lat, lon, radius):
    """Fetch OpenStreetMap POIs for given query."""
    q = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      node["shop"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    try:
        r = requests.get("https://overpass-api.de/api/interpreter", params={"data": q}, timeout=60)
        data = r.json()
    except Exception:
        return []

    results = []
    for el in data.get("elements", []):
        tags = el.get("tags", {}) or {}
        lat_el = el.get("lat") or el.get("center", {}).get("lat")
        lon_el = el.get("lon") or el.get("center", {}).get("lon")
        results.append({
            "name": tags.get("name", "N/A"),
            "type": tags.get("amenity") or tags.get("shop") or "N/A",
            "website": tags.get("website", "N/A"),
            "email": tags.get("email", "N/A"),
            "phone": tags.get("phone", "N/A"),
            "address": tags.get("addr:full", tags.get("addr:street", "N/A")),
            "latitude": lat_el,
            "longitude": lon_el,
        })
    return results

def scrape_website(website):
    """Extract emails and social media links from a given website."""
    emails = []
    socials = {"facebook": "N/A", "instagram": "N/A", "linkedin": "N/A", "twitter": "N/A", "youtube": "N/A"}
    site = normalize_url(website)
    if not site:
        return emails, socials

    try:
        r = requests.get(site, timeout=10)
        html = r.text
        emails = list(set(re.findall(EMAIL_RE, html)))
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "facebook.com" in href: socials["facebook"] = href
            elif "instagram.com" in href: socials["instagram"] = href
            elif "linkedin.com" in href: socials["linkedin"] = href
            elif "twitter.com" in href: socials["twitter"] = href
            elif "youtube.com" in href: socials["youtube"] = href
    except Exception:
        pass
    return emails, socials

# ---------------- Sidebar Inputs ----------------
st.sidebar.header("🔍 Lead Parameters")
country = st.sidebar.text_input("Country", "Italy")
city = st.sidebar.text_input("City", "Rome")
queries = st.sidebar.text_input("Business types", "cafe, restaurant, bar")
radius = st.sidebar.slider("Base radius (m)", 500, 5000, 1000, 100)
steps = st.sidebar.number_input("Radius steps", 1, 5, 2)
verify_sites = st.sidebar.checkbox("Scrape Websites (slow)", value=False)
show_map = st.sidebar.checkbox("Show Map", value=True)
generate = st.sidebar.button("Generate Leads 🚀")

# ---------------- Main Process ----------------
if "leads" not in st.session_state:
    st.session_state["leads"] = pd.DataFrame()

if generate:
    coords = geocode_city(city, country)
    if not coords:
        st.error("❌ Could not locate city.")
        st.stop()

    lat, lon = coords
    st.info(f"📍 Coordinates: {lat:.5f}, {lon:.5f}")

    all_data = []
    for q in [x.strip() for x in queries.split(",") if x.strip()]:
        for step in range(steps):
            r = radius * (step + 1)
            st.write(f"Fetching `{q}` within {r}m radius ...")
            data = fetch_osm_data(q, lat, lon, r)
            if data:
                all_data.extend(data)
                break

    df = pd.DataFrame(all_data)
    if df.empty:
        st.warning("⚠️ No data found.")
        st.stop()

    df["website"] = df["website"].astype(str).replace({"None": "N/A"})

    if verify_sites:
        st.write("🔍 Scraping websites for emails & socials...")
        emails, socials = [], []
        for site in df["website"]:
            e, s = scrape_website(site)
            emails.append(", ".join(e) if e else "N/A")
            socials.append(s)
        df["scraped_emails"] = emails
        df = pd.concat([df, pd.DataFrame(socials)], axis=1)

    df["google_maps"] = df.apply(
        lambda r: f"https://www.google.com/maps?q={r['latitude']},{r['longitude']}" if pd.notna(r["latitude"]) else "N/A",
        axis=1,
    )
    df["city"], df["country"] = city, country
    st.session_state["leads"] = df
    st.success(f"✅ Found {len(df)} leads!")

# ---------------- Display Data ----------------
df = st.session_state["leads"]
if not df.empty:
    st.subheader("📊 Leads Data")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(editable=False, filter=True, sortable=True)

    link_renderer = JsCode("""
    function(params){
      if(!params.value) return "N/A";
      var v=params.value;
      if(v.startsWith("http")) return `<a href="${v}" target="_blank">${v}</a>`;
      return v;
    }
    """)
    gb.configure_column("website", cellRenderer=link_renderer)
    gb.configure_column("google_maps", cellRenderer=link_renderer)

    AgGrid(
        df,
        gridOptions=gb.build(),
        allow_unsafe_jscode=True,
        height=450,
        fit_columns_on_grid_load=True
    )

    if show_map:
        st.subheader("🗺️ Map View")
        m = folium.Map(location=[df["latitude"].mean(), df["longitude"].mean()], zoom_start=12)
        cluster = MarkerCluster().add_to(m)
        for _, r in df.iterrows():
            if pd.notna(r["latitude"]) and pd.notna(r["longitude"]):
                folium.Marker(
                    [r["latitude"], r["longitude"]],
                    popup=f"<b>{r['name']}</b><br>{r['address']}<br><a href='{r['google_maps']}' target='_blank'>Google Maps</a>"
                ).add_to(cluster)
        st_folium(m, width=700, height=450)

    # ---------------- Download ----------------
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    st.download_button("⬇️ Download Excel", out.getvalue(), file_name=f"OSM_Leads_{city}.xlsx")

else:
    st.info("ℹ️ No leads generated yet — set parameters and click 'Generate Leads 🚀'")
