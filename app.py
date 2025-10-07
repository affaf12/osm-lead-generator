# app_upgraded.py
import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import requests
import json
import os
import time
import hashlib
import re
from io import BytesIO
from bs4 import BeautifulSoup
import tldextract
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import nest_asyncio
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# -------------------------
# Basic setup
# -------------------------
nest_asyncio.apply()
st.set_page_config(layout="wide", page_title="🌐 OSM Pro Lead Generator (Upgraded)")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

geolocator = Nominatim(user_agent="StreamlitOSMPro/Upgraded (your-email@example.com)")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

FALLBACK_COORDINATES = {
    "Rome, Italy": (41.902782, 12.496366),
    "Milan, Italy": (45.464203, 9.189982),
    "London, UK": (51.507351, -0.127758),
    "Paris, France": (48.856613, 2.352222),
}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# -------------------------
# Utilities & caching
# -------------------------
def disk_cache_path(key: str):
    h = hashlib.md5(key.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.json")

@st.cache_data(show_spinner=False)
def get_coordinates(location: str):
    """
    Uses geopy with lightweight caching (disk file).
    """
    cache_file = os.path.join(CACHE_DIR, "coords.json")
    if os.path.exists(cache_file):
        try:
            cache = json.load(open(cache_file, "r"))
        except:
            cache = {}
    else:
        cache = {}

    if location in cache:
        return tuple(cache[location])

    try:
        loc = geocode(location)
        if loc:
            cache[location] = (loc.latitude, loc.longitude)
            json.dump(cache, open(cache_file, "w"))
            return loc.latitude, loc.longitude
    except Exception:
        pass

    # try city only
    city_only = location.split(",")[0]
    try:
        loc = geocode(city_only)
        if loc:
            cache[location] = (loc.latitude, loc.longitude)
            json.dump(cache, open(cache_file, "w"))
            return loc.latitude, loc.longitude
    except Exception:
        pass

    return FALLBACK_COORDINATES.get(location, None)

def deduplicate_by_domain(df: pd.DataFrame):
    """
    Keep the highest lead_score row per domain when website present,
    otherwise keep rows without website too.
    """
    df = df.copy()
    def domain_of(url):
        if not url or url in ("N/A", None):
            return None
        ext = tldextract.extract(url)
        if ext.domain:
            return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain
        return None

    df["__domain"] = df["website"].apply(domain_of)
    # sort by lead_score descending so first duplicate kept is best
    df = df.sort_values("__score_sort", ascending=False)
    seen_domains = set()
    keep_rows = []
    no_domain_rows = []

    for _, row in df.iterrows():
        d = row["__domain"]
        if d:
            if d not in seen_domains:
                seen_domains.add(d)
                keep_rows.append(row)
        else:
            no_domain_rows.append(row)

    final = pd.DataFrame(keep_rows + no_domain_rows).drop(columns="__domain")
    return final.reset_index(drop=True)

def score_lead_from_row(row: dict):
    """
    Row is dict-like with keys: emails, facebook,... etc
    """
    score = 0
    emails = row.get("emails")
    if isinstance(emails, list) and len(emails) > 0:
        # prefer unique valid ones
        score += 2
    elif isinstance(emails, str) and emails != "N/A":
        # string may contain a single email
        score += 2

    for k in ("facebook","instagram","linkedin","twitter","tiktok","youtube"):
        v = row.get(k)
        if v and v != "N/A":
            score += 1
    return score

# -------------------------
# OSM fetch (cached)
# -------------------------
def _osm_cache_key(query, lat, lon, radius):
    return f"osm::{query}::{lat:.6f}::{lon:.6f}::{radius}"

def fetch_osm_sync(query: str, lat: float, lon: float, radius: int):
    """
    Synchronous Overpass call (wrapped for asyncio.run compatibility).
    Caches by query+lat+lon+radius on disk.
    """
    key = _osm_cache_key(query, lat, lon, radius)
    cache_path = disk_cache_path(key)
    if os.path.exists(cache_path):
        try:
            return json.load(open(cache_path, "r"))
        except:
            pass

    overpass_url = "https://overpass-api.de/api/interpreter"
    overpass_query = f"""
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
        r = requests.get(overpass_url, params={"data": overpass_query}, timeout=60)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        st.warning(f"Overpass/Network error: {e}")
        data = {"elements": []}

    results = []
    for el in data.get("elements", []):
        tags = el.get("tags", {}) or {}
        lat_el = el.get("lat") or el.get("center", {}).get("lat")
        lon_el = el.get("lon") or el.get("center", {}).get("lon")
        # build address
        addr_parts = []
        for k in ("addr:housenumber","addr:street","addr:city","addr:postcode","addr:country"):
            if tags.get(k):
                addr_parts.append(tags.get(k))
        address = ", ".join(addr_parts) if addr_parts else tags.get("addr:full", "N/A")
        results.append({
            "osm_id": el.get("id"),
            "name": tags.get("name", "N/A"),
            "type": tags.get("amenity") or tags.get("shop") or "N/A",
            "latitude": lat_el,
            "longitude": lon_el,
            "phone": tags.get("phone", "N/A"),
            "website": tags.get("website", "N/A"),
            "emails": tags.get("email", "N/A"),
            "address": address,
            # social tags sometimes saved here (rare)
            "facebook": tags.get("facebook", "N/A"),
            "instagram": tags.get("instagram", "N/A"),
            "linkedin": tags.get("linkedin", "N/A"),
            "twitter": tags.get("twitter", "N/A"),
            "tiktok": tags.get("tiktok", "N/A"),
            "youtube": tags.get("youtube", "N/A")
        })
    # cache result
    try:
        json.dump(results, open(cache_path, "w"))
    except:
        pass
    return results

async def fetch_all_osm_async(queries, lat, lon, base_radius, steps, expand_until_found=True, max_per_query=500):
    """
    Calls Overpass for each query; expands radius up to steps.
    Returns unique results (by osm_id).
    """
    all_results = []
    seen_ids = set()
    total_tasks = len(queries) * steps
    progress = st.progress(0)
    task_count = 0

    for q in queries:
        st.info(f"Searching: {q}")
        found_this_query = False
        for i in range(steps):
            r = base_radius * (i + 1)
            task_count += 1
            progress.progress(task_count / total_tasks)
            res = await asyncio.get_event_loop().run_in_executor(None, fetch_osm_sync, q, lat, lon, r)
            # collect unique osm ids
            for item in res:
                osm_id = item.get("osm_id")
                if osm_id and osm_id not in seen_ids:
                    seen_ids.add(osm_id)
                    all_results.append(item)
                    if len(all_results) >= max_per_query * len(queries):
                        break
            if res:
                found_this_query = True
                if expand_until_found:
                    # stop expanding radius for this query if we already got results
                    break
            if len(all_results) >= max_per_query * len(queries):
                break
        if not found_this_query:
            st.warning(f"No results for '{q}' up to {base_radius * steps}m")
        if len(all_results) >= max_per_query * len(queries):
            st.info("Reached max results limit; stopping further queries.")
            break

    progress.empty()
    return all_results

# -------------------------
# Website scraping (optional)
# -------------------------
def website_cache_key(url: str):
    return disk_cache_path("website::" + url)

async def fetch_page_emails_and_social(session: aiohttp.ClientSession, url: str, retries=2):
    """Return (emails:list, social:dict)"""
    if not url or url in ("N/A", None):
        return [], {"facebook":"N/A","instagram":"N/A","linkedin":"N/A","twitter":"N/A","tiktok":"N/A","youtube":"N/A"}
    cache_path = website_cache_key(url)
    if os.path.exists(cache_path):
        try:
            cached = json.load(open(cache_path, "r"))
            return cached.get("emails", []), cached.get("social", {})
        except:
            pass

    social_domains = {"facebook": "facebook.com", "instagram":"instagram.com", "linkedin":"linkedin.com",
                      "twitter":"twitter.com", "tiktok":"tiktok.com", "youtube":"youtube.com"}
    social = {k:"N/A" for k in social_domains}
    emails = set()
    urls_to_check = [url]

    for page_url in urls_to_check:
        for attempt in range(retries):
            try:
                async with session.get(page_url, timeout=15) as resp:
                    if resp.status != 200:
                        break
                    text = await resp.text(errors="ignore")
                    # emails
                    for m in EMAIL_RE.findall(text):
                        # filter common no-reply prefixes
                        if not any(m.lower().startswith(p) for p in ("info@", "noreply@", "no-reply@", "admin@", "support@", "contact@")):
                            emails.add(m)
                    soup = BeautifulSoup(text, "html.parser")
                    # find social links and contact/about links
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        for name, domain in social_domains.items():
                            if domain in href:
                                if social.get(name) in (None, "N/A"):
                                    social[name] = href
                                else:
                                    # keep first found
                                    pass
                        # follow relative contact/about links (but avoid crawling too deep)
                        href_l = href.lower()
                        if ("contact" in href_l or "about" in href_l) and href_l.startswith("/"):
                            # build absolute using base domain
                            ext = tldextract.extract(url)
                            base = f"https://{ext.domain}.{ext.suffix}" if ext.suffix else f"https://{ext.domain}"
                            next_url = base + href
                            if next_url not in urls_to_check:
                                urls_to_check.append(next_url)
                    break
            except Exception:
                await asyncio.sleep(0.5)
                continue

    emails = list(sorted(emails))
    try:
        json.dump({"emails": emails, "social": social}, open(cache_path, "w"))
    except:
        pass
    return emails, social

async def gather_website_info(websites: list, concurrency=5, progress_placeholder=None):
    sem = asyncio.Semaphore(concurrency)
    results = []

    async def worker(session: aiohttp.ClientSession, url: str, idx: int):
        async with sem:
            emails, social = await fetch_page_emails_and_social(session, url)
            res = {"website": url, "emails": emails, **social}
            if progress_placeholder:
                progress_placeholder.text(f"Scraped {idx+1}/{len(websites)} websites")
            return res

    async with aiohttp.ClientSession() as session:
        tasks = [worker(session, w, i) for i, w in enumerate(websites)]
        out = await asyncio.gather(*tasks)
    return out

# -------------------------
# UI & Main
# -------------------------
# Left column inputs
col_left, col_right = st.columns([1, 3])

with col_left:
    st.header("Search parameters")
    country_input = st.text_input("Country", value="Italy")
    city_input = st.text_input("City", value="Rome")
    queries_input = st.text_input("Business Types (comma-separated)", value="cafe, restaurant, bar")
    radius = st.number_input("Base radius (meters)", value=1000, min_value=200, max_value=10000, step=100)
    steps = st.number_input("Radius Steps (auto-expand)", value=3, min_value=1, max_value=10, step=1)
    max_per_query = st.number_input("Max results per query (safety)", value=300, min_value=50, max_value=2000, step=50)
    do_scrape = st.checkbox("Try to scrape websites for emails & socials (slower)", value=False)
    concurrency = st.number_input("Scraping concurrency (if enabled)", value=6, min_value=1, max_value=20, step=1)
    generate_button = st.button("Generate Leads 🚀")

with col_right:
    status = st.empty()
    aggrid_placeholder = st.empty()
    download_placeholder = st.empty()

if "leads_df" not in st.session_state:
    st.session_state.leads_df = pd.DataFrame()

if generate_button:
    # reset placeholders and state
    status.info("Resolving coordinates...")
    coords = get_coordinates(f"{city_input}, {country_input}")
    if not coords:
        status.error("Could not resolve coordinates for that city/country.")
        st.stop()
    lat, lon = coords
    status.info(f"Coordinates: {lat:.5f}, {lon:.5f} — fetching OSM data...")

    queries = [q.strip() for q in queries_input.split(",") if q.strip()]
    if not queries:
        status.error("Please provide at least one business type.")
        st.stop()

    # fetch OSM (async but uses run_in_executor for sync Overpass calls)
    loop = asyncio.get_event_loop()
    all_items = loop.run_until_complete(fetch_all_osm_async(queries, lat, lon, radius, steps, expand_until_found=True, max_per_query=max_per_query))
    if not all_items:
        status.warning("No OSM results found.")
        st.session_state.leads_df = pd.DataFrame()
    else:
        # build dataframe
        df = pd.DataFrame(all_items)
        # ensure columns exist
        for c in ("website","emails","facebook","instagram","linkedin","twitter","tiktok","youtube"):
            if c not in df.columns:
                df[c] = "N/A"
        # normalize emails column: if string then keep as-is, if list join with ', '
        def norm_emails(v):
            if isinstance(v, list):
                return ", ".join(v) if v else "N/A"
            if v is None:
                return "N/A"
            return str(v)
        df["emails"] = df["emails"].apply(norm_emails)

        # compute score using the helper (we want numeric)
        df["__score_sort"] = df.to_dict(orient="records")  # temporary
        df["lead_score"] = df.apply(lambda row: score_lead_from_row(row), axis=1).astype(int)
        # For dedupe use __score_sort = lead_score value (we set above to list accidentally); fix:
        df["__score_sort"] = df["lead_score"]

        # dedupe by domain (keep highest score)
        df = deduplicate_by_domain(df)

        # ensure types are serializable: convert numpy / None -> python builtins
        df = df.where(pd.notnull(df), None)

        st.session_state.leads_df = df.reset_index(drop=True)
        status.success(f"Found {len(df)} unique leads (after dedupe).")

    # optionally scrape websites to enrich emails/socials
    if do_scrape and not st.session_state.leads_df.empty:
        status.info("Scraping websites to find emails & social links (this may take a while)...")
        # build websites list (unique website strings, filter N/A/None)
        websites = st.session_state.leads_df["website"].dropna().unique().tolist()
        websites = [w for w in websites if w and w not in ("N/A","None")]

        if websites:
            progress_text = st.empty()
            # scrape async
            loop = asyncio.get_event_loop()
            scrape_results = loop.run_until_complete(gather_website_info(websites, concurrency=int(concurrency), progress_placeholder=progress_text))

            # map results back to df rows
            scrape_map = {r["website"]: r for r in scrape_results}
            def enrich_row(row):
                w = row.get("website")
                if not w or w in ("N/A", None):
                    return row
                res = scrape_map.get(w)
                if not res:
                    return row
                # set emails (join list)
                emails = res.get("emails", [])
                if isinstance(emails, list) and emails:
                    row["emails"] = ", ".join(emails)
                # set socials if present
                for k in ("facebook","instagram","linkedin","twitter","tiktok","youtube"):
                    if res.get(k):
                        row[k] = res[k]
                # recompute score
                row["lead_score"] = score_lead_from_row(row)
                return row

            # apply enrichment
            st.session_state.leads_df = pd.DataFrame([enrich_row(r) for r in st.session_state.leads_df.to_dict(orient="records")])
            status.success("Website scraping completed.")
            progress_text.empty()
        else:
            status.info("No websites discovered to scrape.")

# -------------------------
# Display results in AgGrid
# -------------------------
if not st.session_state.leads_df.empty:
    df_display = st.session_state.leads_df.copy()

    st.subheader(f"🌟 Leads for {city_input}, {country_input} — {len(df_display)} rows")
    # Filters
    min_score = st.slider("Minimum Lead Score", 0, 10, 1, key="min_score")
    search_text = st.text_input("Search Name or Website", value="", key="search_box").strip().lower()

    filtered = df_display[df_display["lead_score"] >= int(min_score)]
    if search_text:
        mask_name = filtered["name"].fillna("").astype(str).str.lower().str.contains(search_text)
        mask_web = filtered["website"].fillna("").astype(str).str.lower().str.contains(search_text)
        filtered = filtered[mask_name | mask_web]

    # prepare for AgGrid: ensure columns types are JSON-serializable
    filtered = filtered.fillna("N/A")
    # convert any non-primitive to strings
    for col in filtered.columns:
        # keep lead_score numeric
        if col == "lead_score":
            filtered[col] = filtered[col].astype(int)
        else:
            filtered[col] = filtered[col].astype(str)

    # JsCode renderers
    link_renderer = JsCode("""
    function(params) {
        var v = params.value;
        if(v && v !== "None" && v !== "N/A") {
            // If it's a comma-separated list of emails, show mailto first item
            if(v.indexOf("@") !== -1 && v.indexOf(",") !== -1) {
                var first = v.split(",")[0].trim();
                return `<a href="mailto:${first}">${v}</a>`;
            }
            // hyperlink websites that look like URLs
            if(v.startsWith("http") || v.startsWith("www")) {
                var href = v.startsWith("http") ? v : "https://"+v;
                return `<a href="${href}" target="_blank" rel="noopener">${v}</a>`;
            }
            return v;
        }
        return "N/A";
    }
    """)

    lead_style = JsCode("""
    function(params) {
        if (params.value >= 5) {
            return {'color':'black','backgroundColor':'#00ff99','fontWeight':'bold'};
        } else if (params.value >= 3) {
            return {'color':'black','backgroundColor':'#ffff66','fontWeight':'bold'};
        } else {
            return {'color':'black','backgroundColor':'#ff8a8a','fontWeight':'bold'};
        }
    };
    """)

    gb = GridOptionsBuilder.from_dataframe(filtered)
    gb.configure_default_column(filter=True, sortable=True, resizable=True, editable=False)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    # configure columns
    if "website" in filtered.columns:
        gb.configure_column("website", cellRenderer=link_renderer, headerName="Website")
    if "emails" in filtered.columns:
        gb.configure_column("emails", cellRenderer=link_renderer, headerName="Emails")
    if "lead_score" in filtered.columns:
        gb.configure_column("lead_score", cellStyle=lead_style, headerName="Lead Score", width=120)

    grid_options = gb.build()

    AgGrid(
        filtered,
        gridOptions=grid_options,
        height=520,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
    )

    # Download Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        filtered.to_excel(writer, index=False)
    download_placeholder.download_button(
        "Download Excel",
        output.getvalue(),
        file_name=f"OSM_Leads_{city_input}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("No leads yet. Fill parameters and click Generate Leads 🚀")
