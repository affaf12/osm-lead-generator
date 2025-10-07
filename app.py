# app_dashboard.py - OSM Lead Generator with Dashboard UI
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
geolocator = Nominatim(user_agent="StreamlitOSMPro/2.0 (muhammadaffaf746@gmail.com)")
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
# Custom CSS for Dashboard Theme
# -------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(to right, #0f2027, #203a43, #2c5364);
        color: #ffffff;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .stButton>button {
        background-color: #ffcc00;
        color: #000000;
        font-weight: bold;
        border-radius: 10px;
        padding: 0.5em 1.2em;
    }
    .stButton>button:hover {
        background-color: #ffaa00;
    }
    .stTextInput>div>input, .stNumberInput>div>input {
        background-color: rgba(255,255,255,0.1);
        color: #ffffff;
        border-radius: 5px;
        border: 1px solid #ffffff33;
        padding: 0.4em;
    }
    .lead-card {
        background-color: rgba(255,255,255,0.05);
        border-radius: 15px;
        padding: 10px;
        margin-bottom: 10px;
        box-shadow: 1px 1px 10px rgba(0,0,0,0.5);
    }
    .lead-score-high {color: #00ff99; font-weight: bold;}
    .lead-score-medium {color: #ffff66; font-weight: bold;}
    .lead-score-low {color: #ff5555; font-weight: bold;}
    </style>
    """,
    unsafe_allow_html=True
)

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

def filter_valid_emails(emails):
    invalid_prefixes = ["info@", "noreply@", "admin@", "support@", "no-reply@", "contact@"]
    valid = [e for e in emails if not any(e.lower().startswith(p) for p in invalid_prefixes)]
    valid = [e for e in valid if "." in e.split("@")[-1]]
    return valid

def deduplicate_by_domain(df):
    df['domain'] = df['website'].apply(lambda x: tldextract.extract(x).domain if x not in ["N/A", None] else None)
    df = df.sort_values(['lead_score'], ascending=False)
    df = df.drop_duplicates(subset=['domain'])
    df.drop(columns=['domain'], inplace=True)
    return df

def score_lead(row):
    score = 0
    if row['emails'] and len(row['emails']) > 0:
        score += 2
    for col in ['facebook','instagram','linkedin','twitter','tiktok','youtube']:
        if row.get(col) and row[col] != "N/A":
            score += 1
    return score

# -------------------------
# Async fetching functions
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
                "original_email": tags.get("email", "N/A"),
                "address": tags.get("addr:full", tags.get("addr:street", ""))
            })
        return results
    except:
        return []

async def fetch_website_data(session, website_url):
    if website_url in ["N/A", None]:
        return [], {k:"N/A" for k in ["facebook","instagram","linkedin","twitter","tiktok","youtube"]}
    hash_name = hashlib.md5(website_url.encode()).hexdigest()
    cache_file = os.path.join(CACHE_DIR, f"{hash_name}.json")
    if os.path.exists(cache_file):
        cached = json.load(open(cache_file, "r"))
        return cached["emails"], cached["social"]
    social_domains = ["facebook.com","instagram.com","linkedin.com","twitter.com","tiktok.com","youtube.com"]
    social_links = {domain.split('.')[0]: "N/A" for domain in social_domains}
    emails = []
    urls_to_check = [website_url]
    for page_url in urls_to_check:
        try:
            async with session.get(page_url, timeout=10) as response:
                text = await response.text()
                emails += re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
                soup = BeautifulSoup(text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a['href']
                    for domain in social_domains:
                        if domain in href:
                            social_links[domain.split('.')[0]] = href
                    if "contact" in href.lower() or "about" in href.lower():
                        if href.startswith("/"):
                            ext = tldextract.extract(website_url)
                            href = f"https://{ext.domain}.{ext.suffix}{href}"
                        if href not in urls_to_check:
                            urls_to_check.append(href)
        except:
            continue
    emails = list(set(filter_valid_emails(emails)))
    json.dump({"emails": emails, "social": social_links}, open(cache_file, "w"))
    return emails, social_links

async def gather_website_data_dashboard(websites, concurrency=5):
    sem = asyncio.Semaphore(concurrency)
    df_live = pd.DataFrame(columns=['name','website','emails','facebook','instagram','linkedin','twitter','tiktok','youtube','lead_score'])

    async def sem_fetch(session, site):
        async with sem:
            emails, social = await fetch_website_data(session, site['website'])
            site['emails'] = ", ".join(emails) if emails else "N/A"
            site.update(social)
            site['lead_score'] = score_lead(site)
            nonlocal df_live
            df_live = pd.concat([df_live, pd.DataFrame([site])], ignore_index=True)
    async with aiohttp.ClientSession() as session:
        tasks = [sem_fetch(session, w) for w in websites]
        await asyncio.gather(*tasks)
    df_live = df_live.sort_values('lead_score', ascending=False).reset_index(drop=True)
    return df_live

async def main_dashboard(country, city, queries, radius, steps, concurrency):
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
    df = await gather_website_data_dashboard(all_results, concurrency)
    return df

# -------------------------
# Sidebar Filters
# -------------------------
st.sidebar.header("Search Parameters")
country_input = st.sidebar.text_input("Country", value="Italy")
city_input = st.sidebar.text_input("City", value="Rome")
queries_input = st.sidebar.text_input("Business Types", value="cafe, restaurant, bar")
radius = st.sidebar.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
steps = st.sidebar.number_input("Radius Steps", value=3, min_value=1, max_value=10, step=1)
concurrency = st.sidebar.number_input("Concurrent Requests", value=5, min_value=1, max_value=20, step=1)
generate_button = st.sidebar.button("Generate Leads 🚀")

# -------------------------
# Main Dashboard Display
# -------------------------
if "leads_df" not in st.session_state:
    st.session_state.leads_df = None

if generate_button:
    queries = [q.strip() for q in queries_input.split(",")]
    df = asyncio.run(main_dashboard(country_input, city_input, queries, radius, steps, concurrency))
    if not df.empty:
        st.session_state.leads_df = df
    else:
        st.session_state.leads_df = None

if st.session_state.leads_df is not None:
    st.subheader(f"🌟 Leads for {city_input}, {country_input}")
    # Filter by lead score
    min_score = st.sidebar.slider("Minimum Lead Score", 0, 10, 1)
    filtered_df = st.session_state.leads_df[st.session_state.leads_df['lead_score'] >= min_score]

    for idx, row in filtered_df.iterrows():
        score_class = "lead-score-low"
        if row['lead_score'] >= 5:
            score_class = "lead-score-high"
        elif row['lead_score'] >= 3:
            score_class = "lead-score-medium"
        st.markdown(
            f"""
            <div class="lead-card">
                <b>{row['name']}</b> (<span class="{score_class}">Score: {row['lead_score']}</span>)<br>
                🌐 <a href="{row['website']}" target="_blank">{row['website']}</a><br>
                📧 {row['emails']}<br>
                📞 {row['phone']}<br>
                🔗 {row['facebook']} | {row['instagram']} | {row['linkedin']} | {row['twitter']} | {row['tiktok']} | {row['youtube']}<br>
                📍 {row['address']}
            </div>
            """, unsafe_allow_html=True
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
