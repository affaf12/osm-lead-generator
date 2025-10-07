import streamlit as st
import pandas as pd
import asyncio
import requests
import json
import os
from io import BytesIO
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import nest_asyncio
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# -------------------------
# Setup
# -------------------------
nest_asyncio.apply()
st.set_page_config(layout="wide", page_title="🌐 OSM Lead Dashboard")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

geolocator = Nominatim(user_agent="StreamlitOSMPro/6.0")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

FALLBACK_COORDINATES = {
    "Rome, Italy": (41.902782, 12.496366),
    "Milan, Italy": (45.464203, 9.189982),
    "London, UK": (51.507351, -0.127758),
    "Paris, France": (48.856613, 2.352222),
}

# -------------------------
# Helper Functions
# -------------------------
@st.cache_data(show_spinner=False)
def get_coordinates(location):
    cache_file = os.path.join(CACHE_DIR, "coords.json")
    coords_cache = {}
    if os.path.exists(cache_file):
        coords_cache = json.load(open(cache_file, "r"))
    if location in coords_cache:
        return coords_cache[location]
    try:
        loc = geocode(location)
        if loc:
            coords_cache[location] = (loc.latitude, loc.longitude)
            json.dump(coords_cache, open(cache_file, "w"))
            return loc.latitude, loc.longitude
    except:
        pass
    city_only = location.split(",")[0]
    try:
        loc = geocode(city_only)
        if loc:
            coords_cache[location] = (loc.latitude, loc.longitude)
            json.dump(coords_cache, open(cache_file, "w"))
            return loc.latitude, loc.longitude
    except:
        pass
    return FALLBACK_COORDINATES.get(location, None)

def score_lead(row):
    score = 0
    if row.get('emails') and row['emails'] != "N/A":
        score += 2
    for col in ['facebook','instagram','linkedin','twitter','tiktok','youtube']:
        if row.get(col) and row[col] != "N/A":
            score += 1
    return score

async def fetch_osm(query, lat, lon, radius):
    url = "https://overpass-api.de/api/interpreter"
    q = f"""
    [out:json][timeout:60];
    (
      node["amenity"="{query}"](around:{radius},{lat},{lon});
      way["amenity"="{query}"](around:{radius},{lat},{lon});
      relation["amenity"="{query}"](around:{radius},{lat},{lon});
    );
    out center tags;
    """
    try:
        r = requests.get(url, params={"data": q}, timeout=30)
        data = r.json()
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
                "facebook":"N/A","instagram":"N/A","linkedin":"N/A",
                "twitter":"N/A","tiktok":"N/A","youtube":"N/A"
            })
        return results
    except:
        return []

async def fetch_all_osm(queries, lat, lon, radius, steps):
    all_results = []
    for q in queries:
        for r in [radius*(i+1) for i in range(steps)]:
            res = await fetch_osm(q, lat, lon, r)
            all_results.extend(res)
    return all_results

# -------------------------
# UI
# -------------------------
st.title("🌐 OSM Lead Generator - AgGrid Table")

with st.expander("Search Parameters", expanded=True):
    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        country_input = st.text_input("Country", value="Italy")
        radius = st.number_input("Radius (meters)", value=1000, min_value=500, max_value=5000, step=100)
    with col2:
        city_input = st.text_input("City", value="Rome")
        steps = st.number_input("Radius Steps", value=2, min_value=1, max_value=10, step=1)
    with col3:
        queries_input = st.text_input("Business Types", value="cafe, restaurant, bar")
        generate_button = st.button("Generate Leads 🚀")

if "leads_df" not in st.session_state:
    st.session_state.leads_df = pd.DataFrame()

# -------------------------
# Generate Leads
# -------------------------
if generate_button:
    coords = get_coordinates(f"{city_input}, {country_input}")
    if not coords:
        st.warning("Could not find coordinates!")
        st.stop()
    lat, lon = coords
    queries = [q.strip() for q in queries_input.split(",")]

    # Async fetch
    all_results = asyncio.run(fetch_all_osm(queries, lat, lon, radius, steps))

    if all_results:
        df = pd.DataFrame(all_results)
        df = df.fillna("N/A").astype(str)  # All string values
        df['lead_score'] = df.apply(score_lead, axis=1).astype(int)
        df = df.sort_values('lead_score', ascending=False).reset_index(drop=True)
        st.session_state.leads_df = df
    else:
        st.warning("No results found!")
        st.session_state.leads_df = pd.DataFrame()

# -------------------------
# Display AgGrid Table
# -------------------------
if not st.session_state.leads_df.empty:
    st.subheader(f"🌟 Leads for {city_input}, {country_input}")

    # Filters
    min_score = st.slider("Minimum Lead Score", 0, 10, 1)
    search_text = st.text_input("Search Name/Website", "").lower()

    filtered_df = st.session_state.leads_df[
        st.session_state.leads_df['lead_score'] >= min_score
    ]
    if search_text:
        filtered_df = filtered_df[
            filtered_df['name'].str.lower().str.contains(search_text) |
            filtered_df['website'].str.lower().str.contains(search_text)
        ]

    # Make website/email clickable
    cell_renderer_link = JsCode('''
    function(params) {
        if(params.value && params.value != "N/A") {
            return `<a href="${params.value}" target="_blank">${params.value}</a>`;
        } else {
            return "N/A";
        }
    };
    ''')

    gb = GridOptionsBuilder.from_dataframe(filtered_df)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    gb.configure_default_column(editable=False, filter=True, sortable=True)
    gb.configure_column("lead_score", cellStyle=lambda x: {
        'color':'black',
        'backgroundColor':'#00ff99' if x >=5 else '#ffff66' if x>=3 else '#ff5555',
        'fontWeight':'bold'
    })
    gb.configure_column("website", cellRenderer=cell_renderer_link)
    gb.configure_column("emails", cellRenderer=cell_renderer_link)
    gridOptions = gb.build()

    AgGrid(
        filtered_df,
        gridOptions=gridOptions,
        height=500,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True
    )

    # Download Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        filtered_df.to_excel(writer, index=False)
    st.download_button(
        "Download Excel",
        output.getvalue(),
        file_name=f"OSM_Leads_{city_input}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
