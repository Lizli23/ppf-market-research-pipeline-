#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrape_ppf_atlanta_v2.py
Upgraded pipeline to populate the exact columns required in the brief.
"""
import os, re, time, sys, argparse, math
from urllib.parse import urlparse, parse_qs, urljoin, unquote
from datetime import datetime, timezone
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PPF-research-bot/2.0)"}

BRAND_WORDS = [
    "xpel","stek","3m","suntek","llumar","avery","kavaca","premiumshield",
    "hexis","orafol","oracal","kpmf","vivvid","teckwrap","garware","龙膜","威固","至尊","量子膜"
]

SERVICE_WORDS = ["ppf","paint protection film","clear bra","tint","window tint","wrap","vinyl wrap","ceramic coat","ceramic coating","detailing"]

CITY_DEFAULTS = ["Atlanta,GA","Marietta,GA","Alpharetta,GA","Duluth,GA","Roswell,GA","Sandy Springs,GA","Norcross,GA","Johns Creek,GA","Smyrna,GA","Decatur,GA"]

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def parse_city_zip(address):
    m = re.search(r",\s*([^,]+),\s*[A-Z]{2}\s*(\d{5})", address or "")
    city = m.group(1).strip() if m else ""
    zipc = m.group(2) if m else ""
    return city, zipc

def clean_phone(s):
    if not s: return ""
    digits = re.sub(r"\D+","",s)
    if len(digits)==10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits)==11 and digits[0]=='1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return s

# ---------------- Google Places -----------------
def gp_text_search(city):
    key = os.getenv("GOOGLE_API_KEY")
    if not key: return []
    base = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    query = f"auto detailing OR paint protection film OR clear bra in {city}"
    params = {"query": query, "key": key}
    out = []
    while True:
        r = requests.get(base, params=params, timeout=20)
        js = r.json()
        for it in js.get("results", []):
            out.append({"name": it.get("name",""), "place_id": it.get("place_id","")})
        token = js.get("next_page_token")
        if not token: break
        time.sleep(2)
        params = {"pagetoken": token, "key": key}
    return out

def gp_place_details(place_id):
    key = os.getenv("GOOGLE_API_KEY")
    base = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,formatted_address,formatted_phone_number,website,rating,user_ratings_total"
    r = requests.get(base, params={"place_id": place_id, "fields": fields, "key": key}, timeout=20)
    res = r.json().get("result", {})
    return {
        "Shop Name": res.get("name",""),
        "Address": res.get("formatted_address",""),
        "Contact Number": clean_phone(res.get("formatted_phone_number","")),
        "Website": res.get("website",""),
        "Google Rating": res.get("rating",""),
        "Google #Reviews": res.get("user_ratings_total","")
    }

# ---------------- Yelp -----------------
def yelp_search(city):
    key = os.getenv("YELP_API_KEY")
    if not key: return []
    url = "https://api.yelp.com/v3/businesses/search"
    hdr = {"Authorization": f"Bearer {key}"}
    params = {
        "location": city,
        "term": "paint protection film clear bra wrap tint auto detailing",
        "categories": "autodetailing,auto_customization,autoglass",
        "limit": 50,
        "sort_by": "best_match"
    }
    r = requests.get(url, headers=hdr, params=params, timeout=20)
    out = []
    for b in r.json().get("businesses", []):
        addr = ", ".join(b.get("location",{}).get("display_address", []))
        out.append({
            "Shop Name": b.get("name",""),
            "Address": addr,
            "Yelp Rating": b.get("rating",""),
            "Yelp #Reviews": b.get("review_count",""),
            "Yelp URL": b.get("url",""),
            "Contact Number": b.get("display_phone","")
        })
    return out

def resolve_website_from_yelp(yelp_url):
    try:
        r = requests.get(yelp_url, timeout=15, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a["href"]
            if "/biz_redir" in href and "url=" in href:
                q = parse_qs(urlparse(href).query)
                if "url" in q:
                    return unquote(q["url"][0])
        for a in soup.select("a[href]"):
            href = a["href"]
            if href.startswith("http") and "yelp.com" not in href:
                return href
    except Exception:
        pass
    return ""

def fetch(url):
    r = requests.get(url, timeout=15, headers=HEADERS)
    r.raise_for_status()
    return r

def detect_brands_and_owner(site_url):
    out = {"brands":"","owner":"","remarks":""}
    if not site_url: return out
    try:
        r = fetch(site_url)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        low = text.lower()
        brands = sorted({b for b in BRAND_WORDS if b in low})
        out["brands"] = ", ".join(brands)
        owner = ""
        m = re.search(r"(Owner|Founder|Manager|Lead Installer|Master Installer)\s*[:\-]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})", text, re.I)
        if m: owner = m.group(2)
        if not owner:
            for a in soup.select("a[href]"):
                t = (a.get_text(" ", strip=True) or "").lower()
                if any(k in t for k in ["about","team","our story","founder","owner","staff"]):
                    href = urljoin(site_url, a["href"])
                    try:
                        rr = fetch(href)
                        tt = BeautifulSoup(rr.text,"html.parser").get_text(" ", strip=True)
                        mm = re.search(r"(Owner|Founder|Manager|Lead Installer)\s*[:\-]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})", tt, re.I)
                        if mm:
                            owner = mm.group(2); break
                        mm2 = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+).{0,20}(Owner|Founder|Manager)", tt, re.I)
                        if mm2:
                            owner = mm2.group(1); break
                    except Exception:
                        pass
        out["owner"] = owner
        hints = []
        if "tesla" in low: hints.append("Tesla specialists")
        if any(w in low for w in ["exotic","ferrari","lamborghini","mclaren","porsche","rolls","bentley"]): hints.append("High-end focus")
        out["remarks"] = ", ".join(hints)
    except Exception:
        pass
    return out

def estimate_monthly_volume(yelp_reviews):
    try:
        r = int(yelp_reviews or 0)
    except Exception:
        r = 0
    return max(1, r // 6) if r > 0 else ""

def cmd_discover(args):
    cities = args.city_list or CITY_DEFAULTS
    rows = []
    for city in cities:
        rows += yelp_search(city)
    by_key = {}
    for row in rows:
        c, z = parse_city_zip(row.get("Address",""))
        row["City"] = c
        row["ZIP"] = z
        by_key[(row.get("Shop Name","").lower(), c.lower())] = row
    g_places = []
    for city in cities:
        g_places += gp_text_search(city)
    for gp in g_places:
        det = gp_place_details(gp["place_id"])
        city, zipc = parse_city_zip(det.get("Address",""))
        key = (det.get("Shop Name","").lower(), city.lower())
        existing = by_key.get(key, {})
        merged = {**existing, **det}
        merged["City"] = city or existing.get("City","")
        merged["ZIP"] = zipc or existing.get("ZIP","")
        by_key[key] = merged
    out = list(by_key.values())
    for r in out:
        if not r.get("Website") and r.get("Yelp URL"):
            r["Website"] = resolve_website_from_yelp(r["Yelp URL"])
        r["Estimated Monthly Volume"] = estimate_monthly_volume(r.get("Yelp #Reviews"))
        r["Last Checked (UTC)"] = now_utc()
    df = pd.DataFrame(out).drop_duplicates(subset=["Shop Name","City"])
    df.to_csv(args.out, index=False)
    print(f"Wrote {len(df)} rows to {args.out}")

def cmd_enrich(args):
    IN = args.infile
    OUT = args.out
    if IN.lower().endswith(".xlsx") or IN.lower().endswith(".xls"):
        df = pd.read_excel(IN)
    else:
        df = pd.read_csv(IN)
    for c in ["Shop Name","Address","City","ZIP","Contact Number","Website","Google Rating","Google #Reviews","Yelp Rating","Yelp #Reviews","Estimated Monthly Volume","Remarks"]:
        if c not in df.columns: df[c] = ""
    owners, brands, remarks_extra = [], [], []
    for _, row in df.iterrows():
        site = str(row.get("Website","")).strip()
        info = detect_brands_and_owner(site) if site.startswith("http") else {"brands":"","owner":"","remarks":""}
        owners.append(info.get("owner",""))
        brands.append(info.get("brands",""))
        rmk = row.get("Remarks", "")
        add = info.get("remarks", "")

        rmk = "" if pd.isna(rmk) else str(rmk).strip()
        add = "" if pd.isna(add) else str(add).strip()

        joined = ", ".join([x for x in (rmk, add) if x])
        remarks_extra.append(joined)
    df["Owner / Contact Person"] = owners
    df["Main PPF Brands Used"] = brands
    df["Remarks"] = remarks_extra
    df["Last Checked (UTC)"] = now_utc()
    ordered = [
        "Shop Name","Address","City","ZIP","Contact Number","Owner / Contact Person",
        "Main PPF Brands Used","Google Rating","Google #Reviews","Yelp Rating","Yelp #Reviews",
        "Estimated Monthly Volume","Remarks","Website","Yelp URL","Last Checked (UTC)"
    ]
    existing = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    df = df[existing + rest]
    with pd.ExcelWriter(OUT, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="Shops")
        pd.DataFrame(columns=["Date","Working Hours","Main Tasks","Findings / Notes"]).to_excel(xw, index=False, sheet_name="Daily Log")
    print(f"Enriched file saved to {OUT}")

def main():
    ap = argparse.ArgumentParser(description="Atlanta PPF research v2")
    sub = ap.add_subparsers(dest="cmd")
    p1 = sub.add_parser("discover")
    p1.add_argument("--city-list", nargs="*", default=None)
    p1.add_argument("--out", required=True)
    p1.set_defaults(func=cmd_discover)
    p2 = sub.add_parser("enrich")
    p2.add_argument("--in", dest="infile", required=True)
    p2.add_argument("--out", required=True)
    p2.set_defaults(func=cmd_enrich)
    args = ap.parse_args()
    if not args.cmd:
        ap.print_help(sys.stderr)
        sys.exit(2)
    args.func(args)

if __name__ == "__main__":
    main()
