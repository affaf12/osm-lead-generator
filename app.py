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
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import requests

# -------------------------
# Setup
# -------------------------
nest_asyncio.apply()
geolocator = Nominatim(user_agent="StreamlitOSMPro/1.1 (muhammadaffaf746@gmail.com)")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# -------------------------
# Permanent coordinates fallback
# -------------------------
FALLBACK_COORDINATES = {
    "Rome, Italy": (41.902782, 12.496366),
    "Milan, Italy": (45.464203, 9.189982),
    "London, UK": (51.507351, -0.127758),
    "Paris, France": (48.856613, 2.352222),
    # Add more cities as needed
}

# -------------------------
# Helper functions
# -------------------------
def get_coordinates(location, retries=3):
    """
    Get coordinates using geopy Nominatim with fallback to hard-coded values.
    """
    # Try geopy with retries
    for attempt in range(retries):
        try:
            loc = geocode(location)
            if loc:
                return loc.latitude, loc.longitude
        except:
            time.sleep(2 ** attempt)

    # Fallback: try city only
    city_only = location.split(",")[0]
    for attempt in range(retries):
        try:
            loc = geocode(city_only)
            if loc:
                return loc.latitude, loc.longitude
        except:
            time.sleep(2 ** attempt)

    # Hard-coded fallback
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
                time.sleep(2)
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

async def gather_website_data(websites, progress_bar, status_text, concurrency=5):
    sem = asyncio.Semaphore(concurrency)
    emails_list, social_list = [], []

    async def sem_fetch(session, url, idx):
        async with sem:
            result = await fetch_website_data(session, url)
            emails_list.append(result[0])
            social_list.append(result[1])
            progress_bar.progress((idx + 1) / len(websites))
            status_text.text(f"Scraping websites... ({idx + 1}/{len(websites)})")
            return result

    async with aiohttp.ClientSession() as session:
        tasks = [sem_fetch(session, w, i) for i, w in enumerate(websites)]
        await asyncio.gather(*tasks)

    return emails_list, social_list

# -------------------------
# Main OSM processing with dual progress
# -------------------------
async def main_osm(country, city, queries, radius_list, concurrency):
    all_results = []

    total_tasks = len(queries) * len(radius_list)
    progress_bar = st.progress(0)
    status_text = st.empty()
    task_count = 0

    location = f"{city}, {country}"
    coords = get_coordinates(location)
    if not coords:
        st.stop()  # stop further execution if coordinates not found
    lat, lon = coords

    for query in queries:
        for r in radius_list:
            task_count += 1
            status_text.text(f"Fetching {query} in {city} with radius {r}m... ({task_count}/{total_tasks})")
            results = await fetch_osm(query, lat, lon, r)
            all_results.extend(results)
            progress_bar.progress(task_count / total_tasks)

    if not all_results:
        st.warning("No results found!")
        return None

    df = pd.DataFrame(all_results)
    websites = df['website'].tolist()

    progress_bar.progress(0)
    status_text.text("Scraping websites for emails and social links...")
    emails_list, social_list = await gather_website_data(websites, progress_bar, status_text, concurrency)

    df['emails'] = emails_list
    for social in ['facebook','instagram','linkedin','twitter','tiktok','youtube']:
        df[social] = [s.get(social, "N/A") for s in social_list]

    df = deduplicate_by_domain(df)
    df['lead_score'] = df.apply(score_lead, axis=1)

    status_text.text("✅ Lead generation complete!")
    progress_bar.progress(1.0)
    return df

# -------------------------
# Streamlit UI
# -------------------------
st.title("🌐 OSM Pro Lead Generator")

country_input = st.text_input("Enter Country Name", value="Italy")
city_input = st.text_input("Enter City Name", value="Rome")
queries_input = st.text_input("Enter Business Types (comma separated)", value="cafe, restaurant, bar")
radius = st.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
steps = st.number_input("Radius Steps", value=3, min_value=1, max_value=10)
concurrency = st.number_input("Concurrent Requests", value=5, min_value=1, max_value=20)

if "leads_df" not in st.session_state:
    st.session_state.leads_df = None

if st.button("Generate Leads 🚀"):
    radius_list = [radius * (i+1) for i in range(steps)]
    queries = [q.strip() for q in queries_input.split(",")]
    df = asyncio.run(main_osm(country_input, city_input, queries, radius_list, concurrency))
    if df is not None and not df.empty:
        st.session_state.leads_df = df
        st.success(f"✅ {len(df)} leads found!")
        st.dataframe(df)
    else:
        st.warning("No leads found!")

if st.session_state.leads_df is not None:
    st.download_button(
        "Download Excel File",
        st.session_state.leads_df.to_excel(index=False),
        file_name=f"OSM_Leads_{city_input}.xlsx"
    )
