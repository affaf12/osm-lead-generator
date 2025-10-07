import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import nest_asyncio
import re
import time
import requests
import json
import os
import hashlib
from io import BytesIO
from bs4 import BeautifulSoup
import tldextract
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# -------------------------
# Setup
# -------------------------
st.set_page_config(layout="wide", page_title="🌐 OSM Lead Dashboard")
nest_asyncio.apply()
geolocator = Nominatim(user_agent="StreamlitOSMPro/4.0 (muhammadaffaf746@gmail.com)")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

FALLBACK_COORDINATES = {
    "Rome, Italy": (41.902782, 12.496366),
    "Milan, Italy": (45.464203, 9.189982),
    "London, UK": (51.507351, -0.127758),
    "Paris, France": (48.856613, 2.352222),
}

# -------------------------
# Custom CSS for Table
# -------------------------
st.markdown("""
<style>
.stApp {background: linear-gradient(to right, #1c1c1c, #2c3e50); color: #ffffff;}
.stButton>button {background-color: #ffcc00; color: #000000; font-weight: bold; border-radius: 10px; padding: 0.5em 1.2em;}
.stButton>button:hover {background-color: #ffaa00;}
.stTextInput>div>input, .stNumberInput>div>input, .stSlider>div>input {background-color: rgba(255,255,255,0.1); color: #ffffff; border-radius: 5px; border: 1px solid #ffffff33; padding: 0.4em;}
.dataframe tbody tr:nth-child(even) {background-color: rgba(255,255,255,0.05);}
.dataframe tbody tr:hover {background-color: rgba(255,255,255,0.15);}
.dataframe thead {background-color: #ffcc00; color: #000000;}
</style>
""", unsafe_allow_html=True)

# -------------------------
# Helper Functions
# -------------------------
def get_coordinates(location, retries=3):
    cache_file = os.path.join(CACHE_DIR, "coords.json")
    coords_cache = {}
    if os.path.exists(cache_file):
        coords_cache = json.load(open(cache_file, "r"))
    if location in coords_cache:
        return coords_cache[location]
    for attempt in range(retries):
        try:
            loc = geocode(location)
            if loc:
                coords_cache[location] = (loc.latitude, loc.longitude)
                json.dump(coords_cache, open(cache_file, "w"))
                return loc.latitude, loc.longitude
        except:
            time.sleep(2 ** attempt)
    city_only = location.split(",")[0]
    for attempt in range(retries):
        try:
            loc = geocode(city_only)
            if loc:
                coords_cache[location] = (loc.latitude, loc.longitude)
                json.dump(coords_cache, open(cache_file, "w"))
                return loc.latitude, loc.longitude
        except:
            time.sleep(2 ** attempt)
    if location in FALLBACK_COORDINATES:
        return FALLBACK_COORDINATES[location]
    st.warning(f"[WARN] Could not get coordinates for '{location}'")
    return None

def score_lead(row):
    score = 0
    if row['emails'] and len(row['emails']) > 0:
        score += 2
    for col in ['facebook','instagram','linkedin','twitter','tiktok','youtube']:
        if row.get(col) and row[col] != "N/A":
            score += 1
    return score

# -------------------------
# Async fetching
# -------------------------
async def fetch_osm(query, lat, lon, radius):
    overpass_url = "https://overpass-api.de/api/interpreter"
    query_text = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      way["amenity"="{query}"](around:{radius},{lat},{lon});
      relation["amenity"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    try:
        response = requests.get(overpass_url, params={"data": query_text}, timeout=30)
        data = response.json()
        results = []
        for element in data.get("elements", []):
            tags = element.get("tags", {})
            results.append({
                "name": tags.get("name", "N/A"),
                "latitude": element.get("lat") or element.get("center", {}).get("lat"),
                "longitude": element.get("lon") or element.get("center", {}).get("lon"),
                "phone": tags.get("phone", "N/A"),
                "website": tags.get("website", "N/A"),
                "emails": tags.get("email", "N/A"),
                "facebook":"N/A","instagram":"N/A","linkedin":"N/A","twitter":"N/A","tiktok":"N/A","youtube":"N/A"
            })
        return results
    except:
        return []

async def main_dashboard_table(country, city, queries, radius, steps):
    coords = get_coordinates(f"{city}, {country}")
    if not coords:
        st.stop()
    lat, lon = coords
    all_results = []
    for q in queries:
        for r in [radius*(i+1) for i in range(steps)]:
            results = await fetch_osm(q, lat, lon, r)
            all_results.extend(results)
    if not all_results:
        st.warning("No results found!")
        return pd.DataFrame()
    df = pd.DataFrame(all_results)
    df['lead_score'] = df.apply(score_lead, axis=1)
    df = df.sort_values('lead_score', ascending=False).reset_index(drop=True)
    return df

# -------------------------
# Main Page Inputs
# -------------------------
st.title("🌐 OSM Lead Generator - Table View with Search & Color Coding")

with st.expander("Search Parameters", expanded=True):
    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        country_input = st.text_input("Country", value="Italy")
        radius = st.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
    with col2:
        city_input = st.text_input("City", value="Rome")
        steps = st.number_input("Radius Steps", value=3, min_value=1, max_value=10, step=1)
    with col3:
        queries_input = st.text_input("Business Types (comma-separated)", value="cafe, restaurant, bar")
        generate_button = st.button("Generate Leads 🚀")

# -------------------------
# Lead Table Display
# -------------------------
if "leads_df" not in st.session_state:
    st.session_state.leads_df = None

if generate_button:
    queries = [q.strip() for q in queries_input.split(",")]
    df = asyncio.run(main_dashboard_table(country_input, city_input, queries, radius, steps))
    if not df.empty:
        st.session_state.leads_df = df
    else:
        st.session_state.leads_df = None

if st.session_state.leads_df is not None:
    st.subheader(f"🌟 Leads for {city_input}, {country_input}")

    # Filters: Lead score + Search
    min_score = st.slider("Minimum Lead Score", 0, 10, 1)
    search_text = st.text_input("Search by Name or Website", value="").lower()

    filtered_df = st.session_state.leads_df[st.session_state.leads_df['lead_score'] >= min_score]
    if search_text:
        filtered_df = filtered_df[filtered_df['name'].str.lower().str.contains(search_text) |
                                  filtered_df['website'].str.lower().str.contains(search_text)]

    # Color coding function
    def color_lead_score(val):
        if val >= 5:
            color = 'background-color: #00ff99; color: black; font-weight:bold'
        elif val >= 3:
            color = 'background-color: #ffff66; color: black; font-weight:bold'
        else:
            color = 'background-color: #ff5555; color: black; font-weight:bold'
        return color

    st.dataframe(
        filtered_df.style.applymap(color_lead_score, subset=['lead_score']),
        use_container_width=True
    )

    # Excel Download
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        filtered_df.to_excel(writer, index=False)
    st.download_button(
        "Download Excel File",
        output.getvalue(),
        file_name=f"OSM_Leads_{city_input}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
