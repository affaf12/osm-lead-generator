"""
🌍 OSM Pro Lead Generator — v8 (Streamlit Cloud Compatible)
✅ Fixes & Improvements:
 - Removed aiohttp (uses requests for Streamlit Cloud)
 - Compatible with Python 3.13
 - JsCode safe styling for AgGrid
 - Integrated email & website scraper (basic)
 - Downloadable Excel output
 - Folium map clustering
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
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from st_aggrid.shared import JsCode
from folium.plugins import MarkerCluster

# ---------------- Setup ----------------
st.set_page_config(page_title="🌍 OSM Pro Lead Generator (v8)", layout="wide")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
geolocator = Nominatim(user_agent="OSMProApp/v8")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# ---------------- Utility ----------------
def get_cached_file(key):
    path = os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")
    return path

def cache_load(key):
    path = get_cached_file(key)
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            return None
    return None

def cache_save(key, obj):
    path = get_cached_file(key)
    try:
        json.dump(obj, open(path, "w"))
    except:
        pass

def geocode_city(city, country):
    key = f"geo::{city},{country}"
    cached = cache_load(key)
    if cached:
        return tuple(cached)
    loc = geocode(f"{city}, {country}")
    if loc:
        coords = (loc.latitude, loc.longitude)
        cache_save(key, coords)
        return coords
    return None

def normalize_url(url):
    if not url or url in ("N/A", None):
        return None
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        url = "http://" + url
    return url

# ---------------- Scraper ----------------
def scrape_website(website):
    emails = []
    social_links = {"facebook": "N/A", "instagram": "N/A", "linkedin": "N/A", "twitter": "N/A", "youtube": "N/A"}
    if not website or website == "N/A":
        return emails, social_links
    try:
        r = requests.get(website, timeout=10)
        html = r.text
        emails = list(set(re.findall(EMAIL_RE, html)))
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "facebook.com" in href:
                social_links["facebook"] = href
            elif "instagram.com" in href:
                social_links["instagram"] = href
            elif "linkedin.com" in href:
                social_links["linkedin"] = href
            elif "twitter.com" in href:
                social_links["twitter"] = href
            elif "youtube.com" in href:
                social_links["youtube"] = href
    except:
        pass
    return emails, social_links

# ---------------- OSM Fetch ----------------
def fetch_osm_data(query, lat, lon, radius):
    key = f"osm::{query}::{lat:.4f}::{lon:.4f}::{radius}"
    cached = cache_load(key)
    if cached:
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
        data = r.json()
    except:
        data = {"elements": []}

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
    cache_save(key, results)
    return results

# ---------------- Sidebar ----------------
st.sidebar.title("🔎 OSM Pro Lead Generator v8")
country = st.sidebar.text_input("Country", "Italy")
city = st.sidebar.text_input("City", "Rome")
queries = st.sidebar.text_input("Business types", "cafe, restaurant, bar")
radius = st.sidebar.slider("Base radius (m)", 500, 5000, 1000, 100)
steps = st.sidebar.number_input("Radius steps", 1, 5, 2)
verify_sites = st.sidebar.checkbox("Scrape Websites", value=True)
show_map = st.sidebar.checkbox("Show Map", value=True)
generate = st.sidebar.button("Generate Leads 🚀")

# ---------------- Main ----------------
st.title("🌍 OSM Pro Lead Generator — Streamlit Cloud Version")

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
            results = fetch_osm_data(q, lat, lon, r)
            if results:
                all_data.extend(results)
                break

    df = pd.DataFrame(all_data)
    if df.empty:
        st.warning("⚠️ No data found.")
        st.stop()

    df["website"] = df["website"].astype(str).replace({"None": "N/A"})

    # Scrape websites
    if verify_sites:
        emails, socials = [], []
        for site in stqdm(df["website"], "Scraping sites..."):
            e, s = scrape_website(site)
            emails.append(", ".join(e) if e else "N/A")
            socials.append(s)
        social_df = pd.DataFrame(socials)
        df = pd.concat([df, social_df], axis=1)
        df["scraped_emails"] = emails

    df["google_maps"] = df.apply(
        lambda r: f"https://www.google.com/maps?q={r['latitude']},{r['longitude']}" if pd.notna(r["latitude"]) else "N/A",
        axis=1,
    )
    df["city"] = city
    df["country"] = country

    st.session_state["leads"] = df
    st.success(f"✅ Found {len(df)} leads!")

df = st.session_state["leads"]

if not df.empty:
    st.subheader("📊 Leads Data")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(editable=False, filter=True, sortable=True)
    gb.configure_side_bar()

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

    AgGrid(df, gridOptions=gb.build(), allow_unsafe_jscode=True, height=450, fit_columns_on_grid_load=True)

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

    # Download Excel
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    st.download_button("⬇️ Download Excel", out.getvalue(), file_name=f"OSM_Leads_{city}.xlsx")

else:
    st.info("ℹ️ No leads generated yet — set parameters and click 'Generate Leads 🚀'")
