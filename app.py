# app_pro_v6.py
"""
OSM Pro Lead Generator — All-in-One (v6)
Features:
 - Fetch businesses from Overpass (OpenStreetMap)
 - Optional website scraping for additional emails (async, cached)
 - AI-style scoring heuristic
 - AgGrid table (with safe configuration & allow_unsafe_jscode=True)
 - Optional interactive Folium map preview (click row to center map)
 - Auto-save/restore last search (local cache)
 - Excel export (optionally only leads with emails)
"""
import os
import re
import json
import hashlib
import asyncio
from io import BytesIO

import requests
import pandas as pd
import streamlit as st
import nest_asyncio
import aiohttp
import folium
import tldextract
from streamlit_folium import st_folium
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

nest_asyncio.apply()

# --------------------
# Config & constants
# --------------------
st.set_page_config(page_title="🌍 OSM Pro Lead Generator (v6)", layout="wide")
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
geolocator = Nominatim(user_agent="OSMProApp/v6")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# --------------------
# Utility functions
# --------------------
def _cache_file_key(key: str):
    return os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")

@st.cache_data(show_spinner=False)
def geocode_cached(location: str):
    """Resolve coordinates with geopy and simple disk caching."""
    try:
        path = _cache_file_key("coords")
        if os.path.exists(path):
            d = json.load(open(path))
        else:
            d = {}
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

def normalize_url(url: str):
    if not url or url in ("N/A", None):
        return None
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        url = "http://" + url
    return url

# --------------------
# Overpass fetch
# --------------------
def fetch_overpass(query: str, lat: float, lon: float, radius: int):
    """Synchronous Overpass API call with caching."""
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
            "address": ", ".join([tags.get(k) for k in ("addr:housenumber","addr:street","addr:city","addr:postcode") if tags.get(k)]) or tags.get("addr:full", "N/A"),
            "latitude": lat_el,
            "longitude": lon_el,
        })
    _dump_cache(key, results)
    return results

# --------------------
# Website scraping (async)
# --------------------
async def _fetch_page(session: aiohttp.ClientSession, url: str, retries=2):
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status != 200:
                return ""
            text = await resp.text(errors="ignore")
            return text
    except Exception:
        if retries > 0:
            await asyncio.sleep(0.3)
            return await _fetch_page(session, url, retries-1)
        return ""

async def scrape_website_for_emails(url: str, session: aiohttp.ClientSession):
    if not url:
        return []
    url = normalize_url(url)
    if not url:
        return []
    # check cache first
    cache_key = "site::" + url
    c = _load_cache(cache_key)
    if c is not None:
        return c.get("emails", [])
    text = await _fetch_page(session, url)
    emails = list(set(EMAIL_RE.findall(text or "")))
    # filter generic addresses optionally
    filtered = [e for e in emails if not e.lower().startswith(("noreply","no-reply","info","admin","support","contact"))]
    _dump_cache(cache_key, {"emails": filtered})
    return filtered

async def enrich_websites_async(websites: list, concurrency: int = 5):
    results = {}
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession() as session:
        async def worker(url):
            async with sem:
                try:
                    em = await scrape_website_for_emails(url, session)
                except Exception:
                    em = []
                results[url] = em
        tasks = [worker(u) for u in websites]
        await asyncio.gather(*tasks)
    return results

# --------------------
# Scoring + dedupe
# --------------------
def score_lead(row):
    score = 0
    emails = row.get("emails")
    if isinstance(emails, str) and emails not in ("N/A", ""):
        emails_list = [e.strip() for e in emails.split(",") if "@" in e]
    elif isinstance(emails, list):
        emails_list = emails
    else:
        emails_list = []

    for e in emails_list:
        local = e.split("@")[0]
        if re.match(r"^(info|admin|support|contact|noreply)", local, re.I):
            score += 1
        else:
            score += 3

    for s in ("facebook","instagram","linkedin","twitter","tiktok","youtube"):
        if row.get(s) and row.get(s) != "N/A":
            score += 1

    if row.get("website") and row.get("website") not in ("N/A", ""):
        score += 2

    return int(score)

def dedupe_by_domain(df: pd.DataFrame):
    # keep highest score per domain (if website exists)
    df = df.copy()
    def domain(u):
        if not u or u in ("N/A", None):
            return None
        ext = tldextract.extract(u)
        if ext.domain:
            return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain
        return None
    df["_domain"] = df["website"].apply(domain)
    df = df.sort_values("lead_score", ascending=False)
    keep = []
    seen = set()
    for _, row in df.iterrows():
        d = row["_domain"]
        if d:
            if d not in seen:
                seen.add(d)
                keep.append(row)
        else:
            keep.append(row)
    out = pd.DataFrame(keep).drop(columns=["_domain"])
    return out.reset_index(drop=True)

# --------------------
# Sidebar UI
# --------------------
st.sidebar.title("🔎 OSM Pro Lead Generator — v6")
country = st.sidebar.text_input("Country", "Italy")
city = st.sidebar.text_input("City", "Rome")
queries = st.sidebar.text_input("Business types (comma separated)", "cafe, restaurant, bar")
radius = st.sidebar.slider("Base radius (meters)", 500, 5000, 1000, 100)
steps = st.sidebar.number_input("Radius expansion steps (stop when results)", min_value=1, max_value=5, value=2)
scrape_sites = st.sidebar.checkbox("Scrape websites for emails (slower)", value=False)
scrape_concurrency = st.sidebar.number_input("Scrape concurrency", 2, 20, 6)
show_map = st.sidebar.checkbox("Show interactive map", value=True)
download_only_with_email = st.sidebar.checkbox("Download only leads with emails", value=False)
generate = st.sidebar.button("Generate Leads 🚀")

# autosave last search
LAST_SEARCH_KEY = "last_search_v6"
def save_last_search(params: dict):
    _dump_cache(LAST_SEARCH_KEY, params)
def load_last_search():
    return _load_cache(LAST_SEARCH_KEY) or {}

# restore last search into UI (only first run)
if st.session_state.get("_restored") is None:
    last = load_last_search()
    if last:
        # set widget defaults if present in last search
        try:
            if "country" in last: st.session_state.setdefault("country", last["country"])
            if "city" in last: st.session_state.setdefault("city", last["city"])
        except Exception:
            pass
    st.session_state["_restored"] = True

# main content area
st.title("🌍 OSM Pro Lead Generator — All-in-One v6")
st.markdown("Use the sidebar to configure a search. Optional website scraping can expand email coverage (slower).")

# store leads in session_state
if "leads" not in st.session_state:
    st.session_state["leads"] = pd.DataFrame()

# --------------------
# Generate action
# --------------------
if generate:
    # Save last search
    save_last_search({"country": country, "city": city, "queries": queries, "radius": radius})
    coords = geocode_cached(f"{city}, {country}")
    if not coords:
        st.error("Could not resolve the city/country. Try a different query.")
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
                # stop expanding radius for this query after first results to speed up
                break

    if not all_results:
        st.warning("No OSM results found for your parameters.")
        st.session_state["leads"] = pd.DataFrame()
    else:
        df = pd.DataFrame(all_results)
        # normalize
        if "emails" not in df.columns:
            df["emails"] = "N/A"
        # prepare website column to have valid scheme
        df["website"] = df["website"].astype(str).replace({"None":"N/A"})
        df["website"] = df["website"].apply(lambda u: normalize_url(u) or "N/A")
        df["lead_score"] = df.apply(score_lead, axis=1)
        df = dedupe_by_domain(df)
        st.session_state["leads"] = df
        st.success(f"Found {len(df)} leads (after dedupe).")

    # optional website scraping
    if scrape_sites and not st.session_state["leads"].empty:
        st.info("Scraping websites for emails (this will take time depending on concurrency)...")
        unique_sites = [u for u in st.session_state["leads"]["website"].unique() if u and u not in ("N/A", None)]
        if unique_sites:
            scrape_results = asyncio.run(enrich_websites_async(unique_sites, concurrency=scrape_concurrency))
            # map back
            def map_emails(ws):
                if ws in scrape_results:
                    ems = scrape_results[ws]
                    return ", ".join(ems) if ems else st.session_state["leads"].loc[st.session_state["leads"]["website"]==ws, "emails"].iloc[0]
                return st.session_state["leads"].loc[st.session_state["leads"]["website"]==ws, "emails"].iloc[0]
            st.session_state["leads"]["emails"] = st.session_state["leads"]["website"].apply(map_emails)
            st.session_state["leads"]["lead_score"] = st.session_state["leads"].apply(score_lead, axis=1)
            st.success("Website scraping/enrichment completed.")

# --------------------
# Display results
# --------------------
df = st.session_state["leads"]
if not df.empty:
    st.subheader("Results")
    # show count and quick stats
    col1, col2, col3 = st.columns([1,1,1])
    col1.metric("Leads", len(df))
    col2.metric("Avg score", f"{df['lead_score'].mean():.2f}")
    col3.metric("With website", f"{(df['website']!='N/A').sum()}")

    display_df = df.copy().fillna("N/A")

    # AgGrid configuration with safe JsCode object and allow_unsafe_jscode=True
    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    # clickable website and email renderer
    link_js = JsCode("""
    function(params) {
        if(!params.value) { return "N/A"; }
        var v = params.value;
        if(v.indexOf("@") !== -1) {
            var first = v.split(",")[0].trim();
            return `<a href="mailto:${first}">${v}</a>`;
        } else if(v.startsWith("http")) {
            return `<a href="${v}" target="_blank" rel="noopener">${v}</a>`;
        } else {
            return v;
        }
    }
    """)
    gb.configure_column("website", cellRenderer=link_js)
    gb.configure_column("emails", cellRenderer=link_js)

    # lead score color style
    score_style = JsCode("""
    function(params) {
        if (params.value >= 8) return {'backgroundColor':'#7CFC00','fontWeight':'bold'};
        if (params.value >= 4) return {'backgroundColor':'#FFFF66','fontWeight':'bold'};
        return {'backgroundColor':'#FF9999','fontWeight':'bold'};
    }
    """)
    gb.configure_column("lead_score", cellStyle=score_style, width=120)

    grid_options = gb.build()

    grid_response = AgGrid(
        display_df,
        gridOptions=grid_options,
        height=450,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True
    )

    selected = grid_response.get("selected_rows", [])
    if selected:
        lead = selected[0]
        latc = lead.get("latitude")
        lonc = lead.get("longitude")
        if latc and lonc and show_map:
            # build folium map centered at selected lead
            m = folium.Map(location=[latc, lonc], zoom_start=16)
            folium.Marker([latc, lonc], popup=lead.get("name")).add_to(m)
            st.subheader("Lead location")
            st_folium(m, width=700, height=400)
        else:
            st.info("Selected lead has no coordinates for map preview.")

    # downloads
    # filter for download if user asked only with emails
    if download_only_with_email:
        df_download = display_df[display_df["emails"].notna() & (display_df["emails"].astype(str).str.lower() != "n/a")]
    else:
        df_download = display_df

    if df_download.empty:
        st.warning("No leads match download criteria (e.g., no emails). Download disabled.")
    else:
        out = BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df_download.to_excel(writer, index=False)
        st.download_button("⬇️ Download Excel", out.getvalue(), file_name=f"OSM_Leads_{city}.xlsx")

else:
    st.info("No leads yet — enter parameters and click Generate Leads 🚀")

# small note
st.caption("Tip: If map markers or scraping appear slow, reduce concurrency or radius. Keep queries specific (e.g., 'cafe' rather than 'food').")
