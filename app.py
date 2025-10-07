# app_live_sorted.py - OSM Lead Generator with Live Sorted Table
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
st.set_page_config(layout="wide", page_title="🌐 OSM Pro Lead Generator")
nest_asyncio.apply()
geolocator = Nominatim(user_agent="StreamlitOSMPro/1.4 (muhammadaffaf746@gmail.com)")
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
# Async fetch functions
# -------------------------
async def fetch_osm(query, lat, lon, radius, retries=3):
    overpass_url = "https://overpass-api.de/api/interpreter"
    overpass_query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      way["amenity"="{query}"](around:{radius},{lat},{lon});
      relation["amenity"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    for attempt in range(retries):
        try:
            response = requests.get(overpass_url, params={"data": overpass_query}, timeout=30)
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
            time.sleep(2)
    return []

async def fetch_website_data(session, website_url, retries=2):
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
        for attempt in range(retries):
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
                    break
            except:
                continue

    emails = list(set(filter_valid_emails(emails)))
    json.dump({"emails": emails, "social": social_links}, open(cache_file, "w"))
    return emails, social_links

async def gather_website_data_live_sorted(websites, progress_bar, table_placeholder, concurrency=5):
    sem = asyncio.Semaphore(concurrency)
    df_live = pd.DataFrame(columns=['website','emails','facebook','instagram','linkedin','twitter','tiktok','youtube','lead_score'])

    async def sem_fetch(session, url, idx):
        async with sem:
            emails, social = await fetch_website_data(session, url)

            row = {
                'website': url,
                'emails': ", ".join(emails) if emails else "N/A",
                'facebook': social['facebook'],
                'instagram': social['instagram'],
                'linkedin': social['linkedin'],
                'twitter': social['twitter'],
                'tiktok': social['tiktok'],
                'youtube': social['youtube'],
            }
            row['lead_score'] = score_lead(row)

            nonlocal df_live
            df_live = pd.concat([df_live, pd.DataFrame([row])], ignore_index=True)
            df_live = df_live.sort_values(by='lead_score', ascending=False).reset_index(drop=True)
            table_placeholder.dataframe(df_live, use_container_width=True)
            progress_bar.progress((idx + 1) / len(websites))

    async with aiohttp.ClientSession() as session:
        tasks = [sem_fetch(session, w, i) for i, w in enumerate(websites)]
        await asyncio.gather(*tasks)

    return df_live

async def main_osm_live_sorted(country, city, queries, radius_list, concurrency, table_placeholder):
    all_results = []
    total_tasks = len(queries) * len(radius_list)
    progress_bar = st.progress(0)
    task_count = 0

    coords = get_coordinates(f"{city}, {country}")
    if not coords:
        st.stop()
    lat, lon = coords

    for query in queries:
        for r in radius_list:
            task_count += 1
            progress_bar.progress(task_count / total_tasks)
            results = await fetch_osm(query, lat, lon, r)
            all_results.extend(results)

    if not all_results:
        st.warning("No results found!")
        return None

    df = pd.DataFrame(all_results)
    websites = df['website'].tolist()

    progress_bar.progress(0)
    st.info("Scraping websites and updating table live...")
    df_live = await gather_website_data_live_sorted(websites, progress_bar, table_placeholder, concurrency)

    st.success("✅ Lead generation complete!")
    return df_live

# -------------------------
# Streamlit UI
# -------------------------
st.subheader("Search Parameters")
col1, col2 = st.columns([1,2])
with col1:
    country_input = st.text_input("Country", value="Italy")
    city_input = st.text_input("City", value="Rome")
    queries_input = st.text_input("Business Types", value="cafe, restaurant, bar")
    radius = st.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
    steps = st.number_input("Radius Steps", value=3, min_value=1, max_value=10, step=1)
    concurrency = st.number_input("Concurrent Requests", value=5, min_value=1, max_value=20, step=1)
    generate_button = st.button("Generate Leads 🚀")

with col2:
    table_placeholder = st.empty()

if "leads_df" not in st.session_state:
    st.session_state.leads_df = None

if generate_button:
    if not country_input or not city_input or not queries_input:
        st.warning("Please fill all required fields!")
    else:
        radius_list = [radius * (i+1) for i in range(steps)]
        queries = [q.strip() for q in queries_input.split(",")]
        df = asyncio.run(main_osm_live_sorted(country_input, city_input, queries, radius_list, concurrency, table_placeholder))
        if df is not None and not df.empty:
            st.session_state.leads_df = df
        else:
            st.session_state.leads_df = None

if st.session_state.leads_df is not None:
    st.subheader("Final Leads Table")
    st.dataframe(st.session_state.leads_df, use_container_width=True)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        st.session_state.leads_df.to_excel(writer, index=False)
    st.download_button(
        "Download Excel File",
        output.getvalue(),
        file_name=f"OSM_Leads_{city_input}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
