#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ppf_us_50_states_v3_verified.py

US-wide PPF / Tint / Wrap / Detailing shop discovery + verification pipeline.

Key upgrades (v3):
- Yelp + Google Places cross-validation
- Website + Email extraction
- Google Verified status + Place ID
- Robust to timeouts / partial failures
"""

import os, re, sys, time, math, argparse
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (PPF-US-v3)"}
DEFAULT_TERM = "paint protection film ppf clear bra wrap tint auto detailing"
YELP_CATEGORIES = "autodetailing,auto_customization,autoglass,carwindowtinting"

BRAND_WORDS = [
    "xpel","stek","3m","suntek","llumar","avery","kavaca","premiumshield",
    "hexis","orafol","kpmf","ceramic pro","expel","龙膜","威固","量子膜"
]

US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

TOP_CITIES_BY_STATE = {
    "MD": ["Gaithersburg, MD","Rockville, MD","Bethesda, MD","Baltimore, MD"],
    "GA": ["Atlanta, GA","Marietta, GA","Alpharetta, GA","Savannah, GA"],
    "CA": ["Los Angeles, CA","San Diego, CA","San Jose, CA","San Francisco, CA"],
    # 其余州仍然会 fallback 用 state-level + Yelp
}

# ---------- Utilities ----------
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def safe_str(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return str(x).strip()

def clean_phone(s):
    s = safe_str(s)
    d = re.sub(r"\D+", "", s)
    if len(d) == 10:
        return f"({d[:3]}) {d[3:6]}-{d[6:]}"
    return s

# ---------- Yelp ----------
def yelp_search(location, pages=10):
    key = os.getenv("YELP_API_KEY")
    if not key:
        raise RuntimeError("Missing YELP_API_KEY")
    url = "https://api.yelp.com/v3/businesses/search"
    hdr = {"Authorization": f"Bearer {key}"}
    rows = []
    for p in range(pages):
        params = {
            "location": location,
            "term": DEFAULT_TERM,
            "categories": YELP_CATEGORIES,
            "limit": 50,
            "offset": p * 50
        }
        r = requests.get(url, headers=hdr, params=params, timeout=30).json()
        for b in r.get("businesses", []):
            loc = b.get("location", {})
            coords = b.get("coordinates", {})
            rows.append({
                "Shop Name": b.get("name",""),
                "Address": ", ".join(loc.get("display_address", [])),
                "City": loc.get("city",""),
                "State": loc.get("state",""),
                "ZIP": loc.get("zip_code",""),
                "Contact Number": clean_phone(b.get("display_phone","")),
                "Yelp Rating": b.get("rating",""),
                "Yelp #Reviews": b.get("review_count",""),
                "Yelp URL": b.get("url",""),
                "Lat": coords.get("latitude",""),
                "Lng": coords.get("longitude",""),
            })
        time.sleep(0.15)
    return rows

# ---------- Google ----------
def google_text_search(name, city, state, retries=4, timeout=60):
    """
    Google Places Text Search with retries.
    Returns {} if it fails (never crashes the pipeline).
    """
    key = os.getenv("GOOGLE_API_KEY")
    if not key:
        return {}

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    q = f"{name} {city} {state}"

    for attempt in range(retries):
        try:
            resp = requests.get(url, params={"query": q, "key": key}, timeout=timeout)
            js = resp.json() if resp is not None else {}
            results = js.get("results", []) or []
            if not results:
                return {}
            top = results[0] or {}
            return {
                "place_id": top.get("place_id", ""),
                "name": top.get("name", ""),
                "formatted_address": top.get("formatted_address", ""),
                "rating": top.get("rating", ""),
                "user_ratings_total": top.get("user_ratings_total", ""),
                "lat": ((top.get("geometry", {}) or {}).get("location", {}) or {}).get("lat", ""),
                "lng": ((top.get("geometry", {}) or {}).get("location", {}) or {}).get("lng", ""),
            }

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            # exponential backoff
            time.sleep(1.5 * (2 ** attempt))
        except Exception:
            return {}

    return {}


def google_place_details(place_id):
    key = os.getenv("GOOGLE_API_KEY")
    if not key or not place_id:
        return {}
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "website,formatted_phone_number"
    r = requests.get(url, params={
        "place_id": place_id,
        "fields": fields,
        "key": key
    }, timeout=25).json()
    return r.get("result", {})

# ---------- Website parsing ----------
def extract_socials_email(site):
    out = {"Instagram":"","Facebook":"","Email":""}
    if not site.startswith("http"):
        return out
    try:
        html = requests.get(site, headers=HEADERS, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a[href]"):
            h = a.get("href","")
            if "instagram.com" in h and not out["Instagram"]:
                out["Instagram"] = h
            if "facebook.com" in h and not out["Facebook"]:
                out["Facebook"] = h
            if h.startswith("mailto:") and not out["Email"]:
                out["Email"] = h.replace("mailto:","").split("?")[0]
        if not out["Email"]:
            m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", html, re.I)
            if m:
                out["Email"] = m.group(0)
    except Exception:
        pass
    return out

def detect_brands(site):
    if not site.startswith("http"):
        return ""
    try:
        text = requests.get(site, headers=HEADERS, timeout=20).text.lower()
        found = sorted({b for b in BRAND_WORDS if b in text})
        return ", ".join(found)
    except Exception:
        return ""

# ---------- DISCOVER ----------
def cmd_discover(args):
    all_rows = []
    for st in US_STATES:
        cities = TOP_CITIES_BY_STATE.get(st, [f"{st}, USA"])
        for city in cities:
            try:
                all_rows += yelp_search(city, pages=args.yelp_pages_city)
            except Exception as e:
                print("[WARN]", e)
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["Shop Name","Address"])
    df["Last Checked (UTC)"] = now_utc()
    df.to_csv(args.out, index=False)
    print(f"DISCOVER DONE: {len(df)} shops → {args.out}")

# ---------- ENRICH ----------
def cmd_enrich(args):
    df = pd.read_csv(args.infile)

    for col in [
        "Website","Email","Instagram","Facebook",
        "Google Verified","Google Place ID","Main PPF Brands Used"
    ]:
        if col not in df.columns:
            df[col] = ""

    df = df.astype("string")

    for i, row in df.iterrows():
        name, city, state = safe_str(row["Shop Name"]), safe_str(row["City"]), safe_str(row["State"])

        # Google verify
        if not row["Google Verified"]:
            g = {}
            try:
                g = google_text_search(name, city, state)
            except Exception:
                g = {}
            df.at[i,"Google Verified"] = g.get("Google Verified","No")
            df.at[i,"Google Place ID"] = g.get("Google Place ID","")

        # Google details
        if row["Google Place ID"] and not row["Website"]:
            det = google_place_details(row["Google Place ID"])
            if det.get("website"):
                df.at[i,"Website"] = det["website"]
            if det.get("formatted_phone_number"):
                df.at[i,"Contact Number"] = clean_phone(det["formatted_phone_number"])

        # Website → socials + email + brands
        site = safe_str(df.at[i,"Website"])
        if site:
            soc = extract_socials_email(site)
            for k in soc:
                if not df.at[i,k]:
                    df.at[i,k] = soc[k]
            if not df.at[i,"Main PPF Brands Used"]:
                df.at[i,"Main PPF Brands Used"] = detect_brands(site)

        time.sleep(0.05)

    with pd.ExcelWriter(args.out, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="Shops")
        pd.DataFrame(columns=["Date","Working Hours","Main Tasks","Findings / Notes"]).to_excel(
            xw, index=False, sheet_name="Daily Log"
        )

    print(f"ENRICH DONE → {args.out}")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    d = sub.add_parser("discover")
    d.add_argument("--yelp-pages-city", type=int, default=15)
    d.add_argument("--out", required=True)
    d.set_defaults(func=cmd_discover)

    e = sub.add_parser("enrich")
    e.add_argument("--in", dest="infile", required=True)
    e.add_argument("--out", required=True)
    e.set_defaults(func=cmd_enrich)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
