# app.py
import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import nest_asyncio
import re
from datetime import datetime
from bs4 import BeautifulSoup
import tldextract
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import requests

# -------------------------
# Setup
# -------------------------
nest_asyncio.apply()
geolocator = Nominatim(user_agent="StreamlitOSMPro/1.1")

# -------------------------
# Helper functions
# -------------------------
def get_coordinates(location, retries=3):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": location, "format": "json", "limit": 1}
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers={"User-Agent": "StreamlitOSMPro/1.1"}, timeout=10)
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except:
            if attempt < retries - 1:
                import time; time.sleep(2)
            else:
                return None
    return None

def filter_valid_emails(emails):
    invalid_prefixes = ["info@", "noreply@", "admin@", "support@", "no-reply@", "contact@"]
    valid = [e for e in emails if not any(e.lower().startswith(p) for p in invalid_prefixes)]
    valid = [e for e in valid if "." in e.split("@")[-1]]
    return valid

def deduplicate_by_domain(df):
    df['domain'] = df['website'].apply(lambda x: tldextract.extract(x).domain if x not in ["N/A", None] else None)
    df = df.sort_values(['name'], ascending=True)
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
                name = tags.get("name", "N/A")
                phone = tags.get("phone", "N/A")
                website = tags.get("website", "N/A")
                email = tags.get("email", "N/A")
                lat_el = element.get("lat") or element.get("center", {}).get("lat")
                lon_el = element.get("lon") or element.get("center", {}).get("lon")
                address = tags.get("addr:full", tags.get("addr:street", ""))
                results.append({
                    "name": name,
                    "latitude": lat_el,
                    "longitude": lon_el,
                    "phone": phone,
                    "website": website,
                    "original_email": email,
                    "address": address
                })
            return results
        except:
            if attempt < retries - 1:
                import time; time.sleep(2)
            else:
                return []
    return []

async def fetch_website_data(session, website_url, retries=2):
    social_domains = ["facebook.com","instagram.com","linkedin.com","twitter.com","tiktok.com","youtube.com"]
    social_links = {domain.split('.')[0]: "N/A" for domain in social_domains}
    emails = []
    if website_url in ["N/A", None]:
        return emails, social_links
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
                                key = domain.split('.')[0]
                                social_links[key] = href
                        if "contact" in href.lower() or "about" in href.lower():
                            if href.startswith("/"):
                                ext = tldextract.extract(website_url)
                                base_url = f"https://{ext.domain}.{ext.suffix}"
                                href = base_url + href
                            if href not in urls_to_check:
                                urls_to_check.append(href)
                    break
            except:
                continue
    emails = list(set(filter_valid_emails(emails)))
    return emails, social_links

async def gather_website_data(websites, concurrency=5):
    sem = asyncio.Semaphore(concurrency)
    async def sem_fetch(session, url):
        async with sem:
            return await fetch_website_data(session, url)
    async with aiohttp.ClientSession() as session:
        tasks = [sem_fetch(session, w) for w in websites]
        results = await asyncio.gather(*tasks)
    emails_list = [r[0] for r in results]
    social_list = [r[1] for r in results]
    return emails_list, social_list

# -------------------------
# Main OSM processing
# -------------------------
async def main_osm(cities, queries, radius_list, concurrency):
    all_results = []
    for city in cities:
        coords = get_coordinates(city)
        if not coords:
            st.warning(f"Could not get coordinates for {city}")
            continue
        lat, lon = coords
        for query in queries:
            for r in radius_list:
                st.info(f"Fetching {query} in {city} with radius {r}m...")
                results = await fetch_osm(query, lat, lon, r)
                all_results.extend(results)
    
    if not all_results:
        st.warning("No results found!")
        return None

    df = pd.DataFrame(all_results)
    websites = df['website'].tolist()
    emails_list, social_list = await gather_website_data(websites, concurrency)
    df['emails'] = emails_list
    for social in ['facebook','instagram','linkedin','twitter','tiktok','youtube']:
        df[social] = [s.get(social, "N/A") for s in social_list]

    df = deduplicate_by_domain(df)
    df['lead_score'] = df.apply(score_lead, axis=1)

    # Save Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"OSM_Leads_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    return filename

# -------------------------
# Streamlit UI
# -------------------------
st.title("🌐 OSM Pro Lead Generator")

cities_input = st.text_input("Enter Cities (comma separated)", value="Rome, Milan")
queries_input = st.text_input("Enter Business Types (comma separated)", value="cafe, restaurant, bar")
radius = st.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
steps = st.number_input("Radius Steps", value=3, min_value=1, max_value=10)
concurrency = st.number_input("Concurrent Requests", value=5, min_value=1, max_value=20)

if st.button("Start Lead Generation 🚀"):
    st.info("Lead generation started. This may take a while...")

    # Use asyncio.run() safely with nest_asyncio
    cities = [c.strip() for c in cities_input.split(",")]
    queries = [q.strip() for q in queries_input.split(",")]
    radius_list = [radius * (i+1) for i in range(steps)]

    filename = asyncio.run(main_osm(cities, queries, radius_list, concurrency))
    if filename:
        st.success(f"✅ File ready: {filename}")
        with open(filename, "rb") as f:
            st.download_button("Download Excel File", f, file_name=filename)
