#!/usr/bin/env python3
"""
GPU Market Intelligence Scraper — Module 1 (v2)
Collects spot pricing and availability from 4 providers:
  - Vast.ai (marketplace offers)
  - RunPod (GPU types via GraphQL)
  - Lambda Labs (instance types)
  - TensorDock (marketplace hostnodes)

Outputs a single unified CSV: data/gpu_pricing.csv
Each row = one GPU SKU + provider + timestamp
"""

import os
import csv
import json
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────
VAST_API_KEY = os.environ.get("VAST_API_KEY", "")
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY", "")
LAMBDA_API_KEY = os.environ.get("LAMBDA_API_KEY", "")
TENSORDOCK_API_KEY = os.environ.get("TENSORDOCK_API_KEY", "")
TENSORDOCK_API_TOKEN = os.environ.get("TENSORDOCK_API_TOKEN", "")

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_CSV = DATA_DIR / "gpu_pricing.csv"

# Canonical GPU names — we normalize everything to these
GPU_ALIASES = {
    "H100": ["H100", "H100_SXM", "H100_SXM5", "H100_PCIE", "H100 SXM", "H100 80GB SXM5", "H100 PCIe", "gpu_8x_h100_sxm5", "gpu_1x_h100_sxm5"],
    "H200": ["H200", "H200_SXM", "gpu_8x_h200"],
    "A100 80GB": ["A100_SXM4", "A100_SXM", "A100 80GB SXM4", "A100 80GB SXM", "A100_80GB", "A100X", "gpu_8x_a100_80gb_sxm4", "gpu_1x_a100_80gb_sxm4"],
    "A100 40GB": ["A100_PCIE", "A100 40GB", "A100_40GB", "A100 PCIe", "gpu_1x_a100_pcie_40gb"],
    "L40S": ["L40S", "gpu_8x_l40s"],
    "L40": ["L40"],
    "A10": ["A10", "gpu_1x_a10"],
    "RTX 4090": ["RTX_4090", "RTX 4090", "GeForce RTX 4090"],
    "RTX A6000": ["RTX_A6000", "A6000", "RTX A6000"],
    "RTX 3090": ["RTX_3090", "RTX 3090", "GeForce RTX 3090"],
    "GH200": ["GH200", "gpu_1x_gh200"],
    "B200": ["B200", "gpu_8x_b200"],
}

def normalize_gpu(raw_name: str) -> str:
    """Map any GPU name variant to a canonical name."""
    clean = str(raw_name).strip()
    for canonical, aliases in GPU_ALIASES.items():
        for alias in aliases:
            if alias.upper().replace(" ", "_").replace("-", "_") in clean.upper().replace(" ", "_").replace("-", "_"):
                return canonical
            if clean.upper().replace("_", " ") == alias.upper().replace("_", " "):
                return canonical
    # If no match, return cleaned version
    return clean.replace("_", " ").strip()

def is_tracked(raw_name: str) -> bool:
    """Check if this GPU is one we want to track."""
    norm = normalize_gpu(raw_name)
    return norm in GPU_ALIASES

TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ── Vast.ai ──────────────────────────────────────────────────

def scrape_vastai():
    if not VAST_API_KEY:
        print("⚠️  VAST_API_KEY not set — skipping")
        return []

    url = "https://console.vast.ai/api/v0/search/asks/"
    headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": f"Bearer {VAST_API_KEY}"}
    payload = {"q": {"verified": {"eq": True}, "rentable": {"eq": True}, "external": {"eq": False}, "type": "on-demand"}, "limit": 5000}

    print("📡 Vast.ai...")
    try:
        resp = requests.put(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ {e}")
        return []

    offers = data.get("offers", data) if isinstance(data, dict) else data
    if not offers:
        print("  ⚠️  No offers")
        return []

    # Group by normalized GPU name
    groups = {}
    for o in offers:
        raw = o.get("gpu_name", "")
        if not is_tracked(raw):
            continue
        canonical = normalize_gpu(raw)
        num_gpus = max(o.get("num_gpus", 1), 1)
        price = round(o.get("dph_total", 0) / num_gpus, 4)
        geo = o.get("geolocation", "Unknown")
        region = str(geo).split(",")[-1].strip()[:30] if geo else "Unknown"

        if canonical not in groups:
            groups[canonical] = {"prices": [], "regions": {}, "dc": 0}
        groups[canonical]["prices"].append(price)
        groups[canonical]["regions"][region] = groups[canonical]["regions"].get(region, 0) + 1
        if o.get("datacenter"):
            groups[canonical]["dc"] += 1

    rows = []
    for gpu, g in groups.items():
        prices = sorted(g["prices"])
        n = len(prices)
        med = prices[n//2] if n%2 else round((prices[n//2-1]+prices[n//2])/2, 4)
        top_reg = sorted(g["regions"].items(), key=lambda x: -x[1])[:3]

        rows.append({
            "timestamp": TIMESTAMP,
            "provider": "Vast.ai",
            "gpu": gpu,
            "min_price_hr": round(min(prices), 4),
            "median_price_hr": round(med, 4),
            "max_price_hr": round(max(prices), 4),
            "on_demand_hr": "",
            "spot_price_hr": round(min(prices), 4),
            "num_offers": n,
            "availability": "Available" if n > 0 else "Unavailable",
            "top_regions": "; ".join(f"{r}:{c}" for r, c in top_reg),
        })

    print(f"  ✅ {len(rows)} GPU types")
    return rows


# ── RunPod ────────────────────────────────────────────────────

def scrape_runpod():
    if not RUNPOD_API_KEY:
        print("⚠️  RUNPOD_API_KEY not set — skipping")
        return []

    url = "https://api.runpod.io/graphql"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {RUNPOD_API_KEY}"}
    query = """query { gpuTypes { id displayName memoryInGb secureCloud communityCloud
        securePrice communityPrice communitySpotPrice secureSpotPrice
        lowestPrice { minimumBidPrice uninterruptablePrice stockStatus }
        maxGpuCountCommunityCloud maxGpuCountSecureCloud } }"""

    print("📡 RunPod...")
    try:
        resp = requests.post(url, headers=headers, json={"query": query}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ {e}")
        return []

    gpu_types = data.get("data", {}).get("gpuTypes", [])
    rows = []
    for g in gpu_types:
        raw = g.get("displayName", "")
        if not is_tracked(raw):
            continue

        lowest = g.get("lowestPrice") or {}
        community = g.get("communityPrice") or 0
        spot = g.get("communitySpotPrice") or 0
        secure = g.get("securePrice") or 0
        stock = lowest.get("stockStatus", "Unknown")

        rows.append({
            "timestamp": TIMESTAMP,
            "provider": "RunPod",
            "gpu": normalize_gpu(raw),
            "min_price_hr": round(min(filter(None, [community, spot, secure])) or 0, 4),
            "median_price_hr": round(community, 4) if community else "",
            "max_price_hr": round(secure, 4) if secure else "",
            "on_demand_hr": round(community, 4) if community else "",
            "spot_price_hr": round(spot, 4) if spot else "",
            "num_offers": "",
            "availability": stock,
            "top_regions": "",
        })

    print(f"  ✅ {len(rows)} GPU types")
    return rows


# ── Lambda Labs ───────────────────────────────────────────────

def scrape_lambda():
    if not LAMBDA_API_KEY:
        print("⚠️  LAMBDA_API_KEY not set — skipping")
        return []

    url = "https://cloud.lambdalabs.com/api/v1/instance-types"
    headers = {"Authorization": f"Bearer {LAMBDA_API_KEY}"}

    print("📡 Lambda Labs...")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ {e}")
        return []

    instance_types = data.get("data", {})
    if not instance_types:
        print("  ⚠️  No data")
        return []

    rows = []
    for type_name, info in instance_types.items():
        specs = info.get("instance_type", {})
        gpu_desc = specs.get("description", type_name)
        price = specs.get("price_cents_per_hour", 0) / 100
        num_gpus = specs.get("specs", {}).get("gpus", 1)
        price_per_gpu = round(price / max(num_gpus, 1), 4)

        # Check if this is a tracked GPU
        raw_name = specs.get("gpu_description", type_name)
        if not is_tracked(raw_name) and not is_tracked(type_name):
            continue

        canonical = normalize_gpu(raw_name) if is_tracked(raw_name) else normalize_gpu(type_name)

        regions_available = info.get("regions_with_capacity_available", [])
        avail = "Available" if regions_available else "Unavailable"
        region_names = [r.get("description", r.get("name", "")) for r in regions_available]

        rows.append({
            "timestamp": TIMESTAMP,
            "provider": "Lambda",
            "gpu": canonical,
            "min_price_hr": price_per_gpu,
            "median_price_hr": price_per_gpu,
            "max_price_hr": price_per_gpu,
            "on_demand_hr": price_per_gpu,
            "spot_price_hr": "",
            "num_offers": len(regions_available),
            "availability": avail,
            "top_regions": "; ".join(region_names[:3]),
        })

    print(f"  ✅ {len(rows)} GPU types")
    return rows


# ── TensorDock ────────────────────────────────────────────────

def scrape_tensordock():
    if not TENSORDOCK_API_KEY or not TENSORDOCK_API_TOKEN:
        print("⚠️  TENSORDOCK keys not set — skipping")
        return []

    url = "https://marketplace.tensordock.com/api/v0/client/deploy/hostnodes"
    params = {"api_key": TENSORDOCK_API_KEY, "api_token": TENSORDOCK_API_TOKEN}

    print("📡 TensorDock...")
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ {e}")
        return []

    hostnodes = data.get("hostnodes", {})
    if not hostnodes:
        print("  ⚠️  No hostnodes")
        return []

    # Group by GPU type
    groups = {}
    for node_id, node in hostnodes.items():
        specs = node.get("specs", {})
        gpu_info = specs.get("gpu", {})
        location = node.get("location", {})
        region = location.get("country", "Unknown")

        for gpu_model, gpu_details in gpu_info.items():
            if not is_tracked(gpu_model):
                continue
            canonical = normalize_gpu(gpu_model)
            amount = gpu_details.get("amount", 0)
            # TensorDock prices are per GPU per hour
            price = gpu_details.get("price", 0)
            if amount <= 0 or price <= 0:
                continue

            if canonical not in groups:
                groups[canonical] = {"prices": [], "regions": {}, "total_gpus": 0}
            groups[canonical]["prices"].append(price)
            groups[canonical]["total_gpus"] += amount
            groups[canonical]["regions"][region] = groups[canonical]["regions"].get(region, 0) + amount

    rows = []
    for gpu, g in groups.items():
        prices = sorted(g["prices"])
        n = len(prices)
        med = prices[n//2] if n%2 else round((prices[n//2-1]+prices[n//2])/2, 4)
        top_reg = sorted(g["regions"].items(), key=lambda x: -x[1])[:3]

        rows.append({
            "timestamp": TIMESTAMP,
            "provider": "TensorDock",
            "gpu": gpu,
            "min_price_hr": round(min(prices), 4),
            "median_price_hr": round(med, 4),
            "max_price_hr": round(max(prices), 4),
            "on_demand_hr": round(med, 4),
            "spot_price_hr": round(min(prices), 4),
            "num_offers": g["total_gpus"],
            "availability": "Available" if g["total_gpus"] > 0 else "Unavailable",
            "top_regions": "; ".join(f"{r}:{c}" for r, c in top_reg),
        })

    print(f"  ✅ {len(rows)} GPU types")
    return rows


# ── CSV Writer ────────────────────────────────────────────────

FIELDS = ["timestamp", "provider", "gpu", "min_price_hr", "median_price_hr",
          "max_price_hr", "on_demand_hr", "spot_price_hr", "num_offers",
          "availability", "top_regions"]

def append_csv(rows):
    if not rows:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f"  💾 Wrote {len(rows)} rows to {OUTPUT_CSV.name}")


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  GPU Market Intel Scraper v2")
    print(f"  {TIMESTAMP}")
    print("=" * 60)

    all_rows = []
    all_rows.extend(scrape_vastai())
    all_rows.extend(scrape_runpod())
    all_rows.extend(scrape_lambda())
    all_rows.extend(scrape_tensordock())

    append_csv(all_rows)

    # Summary
    providers = set(r["provider"] for r in all_rows)
    gpus = set(r["gpu"] for r in all_rows)
    print(f"\n✅ Scrape complete: {len(all_rows)} rows from {len(providers)} providers covering {len(gpus)} GPU SKUs")


if __name__ == "__main__":
    main()
