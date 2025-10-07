# app_pro.py — Final All-in-One OSM Lead Generator
import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import requests
import json
import os
import re
import hashlib
import nest_asyncio
from io import BytesIO
from bs4 import BeautifulSoup
import tldextract
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# ----------------------------
# Setup & Config
# ----------------------------
st.set_page_config(page_title="🌍 OSM Pro Lead Generator (All-in-One)", layout="wide")
nest_asyncio.apply()

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

geolocator = Nominatim(user_agent="OSMProApp/AI")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# ----------------------------
# Helpers
# ----------------------------
def cache_path(key):
    return os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")

@st.cache_data(show_spinner=False)
def get_coordinates(location):
    """Geocode with caching"""
    cache_file = cache_path("coords")
    if os.path.exists(cache_file):
        try:
            coords = json.load(open(cache_file))
        except:
            coords = {}
    else:
        coords = {}
    if location in coords:
        return coords[location]
    loc = geocode(location)
    if loc:
        coords[location] = (loc.latitude, loc.longitude)
        json.dump(coords, open(cache_file, "w"))
        return coords[location]
    return None

def score_ai_heuristic(row):
    """AI-inspired scoring"""
    score = 0
    email_field = row.get("emails", "")
    if isinstance(email_field, str):
        emails = [e.strip() for e in email_field.split(",") if "@" in e]
    else:
        emails = email_field if isinstance(email_field, list) else []

    # Email quality
    for e in emails:
        if re.match(r"^(info|admin|support|contact|noreply)", e.split("@")[0], re.I):
            score += 1
        else:
            score += 3

    # Social presence
    for s in ["facebook", "instagram", "linkedin", "twitter", "tiktok", "youtube"]:
        if row.get(s) not in (None, "N/A"):
            score += 1.5

    # Website presence
    if row.get("website") not in (None, "N/A"):
        score += 2

    return int(score)

# ----------------------------
# OSM Fetch
# ----------------------------
def fetch_osm(query, lat, lon, radius):
    key = f"osm::{query}::{lat:.4f}::{lon:.4f}::{radius}"
    cache_file = cache_path(key)
    if os.path.exists(cache_file):
        try:
            return json.load(open(cache_file))
        except:
            pass

    overpass_url = "https://overpass-api.de/api/interpreter"
    overpass_query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      node["shop"="{query}"](around:{radius},{lat},{lon});
      way["shop"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    try:
        res = requests.get(overpass_url, params={"data": overpass_query}, timeout=60)
        data = res.json()
    except:
        data = {"elements": []}

    results = []
    for el in data.get("elements", []):
        tags = el.get("tags", {})
        results.append({
            "name": tags.get("name", "N/A"),
            "type": tags.get("shop") or tags.get("amenity", "N/A"),
            "website": tags.get("website", "N/A"),
            "emails": tags.get("email", "N/A"),
            "phone": tags.get("phone", "N/A"),
            "address": tags.get("addr:full", "N/A"),
            "latitude": el.get("lat") or el.get("center", {}).get("lat"),
            "longitude": el.get("lon") or el.get("center", {}).get("lon"),
        })
    json.dump(results, open(cache_file, "w"))
    return results

# ----------------------------
# Async Web Scraping
# ----------------------------
async def fetch_site_info(url, session):
    if not url or url == "N/A":
        return []
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return []
            html = await resp.text(errors="ignore")
            emails = list(set(EMAIL_RE.findall(html)))
            return [e for e in emails if not e.lower().startswith(("info@", "no-reply@", "noreply@", "support@"))]
    except:
        return []

async def enrich_websites(df, concurrency=5):
    urls = [u for u in df["website"].unique() if u not in ("N/A", None)]
    results = {}
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession() as session:
        async def task(url):
            async with sem:
                emails = await fetch_site_info(url, session)
                results[url] = ", ".join(emails) if emails else "N/A"
        await asyncio.gather(*[task(u) for u in urls])
    df["emails"] = df["website"].map(results).fillna(df["emails"])
    return df

# ----------------------------
# UI
# ----------------------------
st.sidebar.header("🔎 Search Parameters")
country = st.sidebar.text_input("Country", "Italy")
city = st.sidebar.text_input("City", "Rome")
queries = st.sidebar.text_input("Business Types (comma separated)", "restaurant, cafe, bar")
radius = st.sidebar.slider("Radius (m)", 500, 5000, 1000, 500)
scrape_web = st.sidebar.checkbox("Scrape websites for emails (slower)", False)
generate = st.sidebar.button("Generate Leads 🚀")

st.title("🌍 OSM Pro Lead Generator — All-in-One Edition")
st.caption("Smart business lead finder powered by OpenStreetMap + AI-like scoring")

if "data" not in st.session_state:
    st.session_state.data = pd.DataFrame()

if generate:
    coords = get_coordinates(f"{city}, {country}")
    if not coords:
        st.error("Could not locate city.")
        st.stop()
    lat, lon = coords
    st.info(f"Coordinates resolved: {lat:.5f}, {lon:.5f}")

    all_data = []
    for q in [x.strip() for x in queries.split(",") if x.strip()]:
        all_data.extend(fetch_osm(q, lat, lon, radius))

    df = pd.DataFrame(all_data)
    if df.empty:
        st.warning("No data found.")
        st.stop()

    if scrape_web:
        st.info("Scraping websites for better emails...")
        df = asyncio.run(enrich_websites(df))

    df["lead_score"] = df.apply(score_ai_heuristic, axis=1)
    st.session_state.data = df
    st.success(f"✅ Found {len(df)} leads.")

if not st.session_state.data.empty:
    df = st.session_state.data.copy()
    df = df.fillna("N/A")

    st.subheader("📊 Results")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=False, wrapText=True)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    gb.configure_column("lead_score", cellStyle=JsCode("""
        function(p){
            if(p.value >= 8) return {'backgroundColor':'#7CFC00','fontWeight':'bold'};
            if(p.value >= 4) return {'backgroundColor':'#FFFF66','fontWeight':'bold'};
            return {'backgroundColor':'#FF9999','fontWeight':'bold'};
        }
    """))
    AgGrid(df, gridOptions=gb.build(), height=500, fit_columns_on_grid_load=True, update_mode=GridUpdateMode.NO_UPDATE)

    # Download Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    st.download_button("⬇️ Download Excel", output.getvalue(), file_name=f"OSM_Leads_{city}.xlsx")

else:
    st.info("👈 Enter parameters and click **Generate Leads 🚀** to begin.")
