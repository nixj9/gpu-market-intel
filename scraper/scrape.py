#!/usr/bin/env python3
"""
GPU Market Intelligence Scraper — Module 1
Collects spot pricing and availability data from Vast.ai and RunPod.
Designed to run on GitHub Actions (hourly cron) and append to CSV files.
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

DATA_DIR = Path(__file__).parent.parent / "data"
VAST_CSV = DATA_DIR / "vast_history.csv"
RUNPOD_CSV = DATA_DIR / "runpod_history.csv"

# GPU models to track — modify as needed
TARGET_GPUS = [
    "H100", "H100_SXM", "H200",
    "A100_SXM4", "A100_PCIE", "A100X",
    "L40S", "L40",
    "RTX_4090", "RTX_A6000",
    "A6000",
]


def matches_target(gpu_name: str) -> bool:
    """Check if a GPU name matches any of our target SKUs."""
    name_upper = str(gpu_name).upper().replace(" ", "_").replace("-", "_")
    for target in TARGET_GPUS:
        if target.upper().replace("-", "_") in name_upper:
            return True
    return False


def normalize_gpu_name(name: str) -> str:
    """Normalize GPU name for consistent grouping."""
    return str(name).strip().replace(" ", "_")


# ── Vast.ai Scraper ──────────────────────────────────────────

def fetch_vastai():
    """Pull Vast.ai marketplace offers and aggregate by GPU type."""
    url = "https://console.vast.ai/api/v0/search/asks/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if VAST_API_KEY:
        headers["Authorization"] = f"Bearer {VAST_API_KEY}"

    payload = {
        "q": {
            "verified": {"eq": True},
            "rentable": {"eq": True},
            "external": {"eq": False},
            "type": "on-demand",
        },
        "limit": 5000,
    }

    print("📡 Fetching Vast.ai offers...")
    try:
        resp = requests.put(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ Vast.ai API error: {e}")
        return []

    offers = data.get("offers", data) if isinstance(data, dict) else data
    if not offers:
        print("  ⚠️  No offers returned")
        return []

    print(f"  Raw offers: {len(offers)}")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Group by GPU name
    gpu_groups = {}
    for o in offers:
        gpu_name = normalize_gpu_name(o.get("gpu_name", "Unknown"))
        if not matches_target(gpu_name):
            continue

        num_gpus = max(o.get("num_gpus", 1), 1)
        price_per_gpu = round(o.get("dph_total", 0) / num_gpus, 4)
        reliability = o.get("reliability2", 0)
        datacenter = o.get("datacenter", False)
        geolocation = o.get("geolocation", "Unknown")

        if gpu_name not in gpu_groups:
            gpu_groups[gpu_name] = {
                "prices": [],
                "reliabilities": [],
                "dc_count": 0,
                "total": 0,
                "geos": {},
            }

        gpu_groups[gpu_name]["prices"].append(price_per_gpu)
        gpu_groups[gpu_name]["reliabilities"].append(reliability)
        gpu_groups[gpu_name]["total"] += 1
        if datacenter:
            gpu_groups[gpu_name]["dc_count"] += 1

        # Track geo distribution
        region = str(geolocation).split(",")[-1].strip()[:30] if geolocation else "Unknown"
        gpu_groups[gpu_name]["geos"][region] = gpu_groups[gpu_name]["geos"].get(region, 0) + 1

    # Build summary rows
    rows = []
    for gpu_name, g in gpu_groups.items():
        prices = sorted(g["prices"])
        n = len(prices)
        median = prices[n // 2] if n % 2 == 1 else round((prices[n // 2 - 1] + prices[n // 2]) / 2, 4)
        top_regions = sorted(g["geos"].items(), key=lambda x: -x[1])[:3]
        top_regions_str = "; ".join(f"{r}:{c}" for r, c in top_regions)

        rows.append({
            "timestamp": timestamp,
            "source": "vast.ai",
            "gpu_name": gpu_name,
            "num_offers": g["total"],
            "min_price_hr": round(min(prices), 4),
            "median_price_hr": round(median, 4),
            "max_price_hr": round(max(prices), 4),
            "p25_price_hr": round(prices[n // 4], 4) if n >= 4 else round(min(prices), 4),
            "p75_price_hr": round(prices[3 * n // 4], 4) if n >= 4 else round(max(prices), 4),
            "avg_reliability": round(sum(g["reliabilities"]) / n, 4),
            "datacenter_count": g["dc_count"],
            "top_regions": top_regions_str,
        })

    print(f"  ✅ {len(rows)} GPU types aggregated")
    return rows


# ── RunPod Scraper ────────────────────────────────────────────

def fetch_runpod():
    """Pull RunPod GPU type pricing via GraphQL."""
    url = "https://api.runpod.io/graphql"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }

    query = """
    query {
        gpuTypes {
            id
            displayName
            manufacturer
            memoryInGb
            secureCloud
            communityCloud
            securePrice
            communityPrice
            communitySpotPrice
            secureSpotPrice
            lowestPrice {
                minimumBidPrice
                uninterruptablePrice
                stockStatus
            }
            maxGpuCount
            maxGpuCountCommunityCloud
            maxGpuCountSecureCloud
        }
    }
    """

    print("📡 Fetching RunPod GPU types...")
    try:
        resp = requests.post(url, headers=headers, json={"query": query}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ RunPod API error: {e}")
        return []

    gpu_types = data.get("data", {}).get("gpuTypes", [])
    if not gpu_types:
        print("  ⚠️  No GPU types returned")
        return []

    print(f"  Raw GPU types: {len(gpu_types)}")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for g in gpu_types:
        display_name = g.get("displayName", "Unknown")
        if not matches_target(display_name):
            continue

        lowest = g.get("lowestPrice") or {}

        rows.append({
            "timestamp": timestamp,
            "source": "runpod",
            "gpu_name": normalize_gpu_name(display_name),
            "gpu_id": g.get("id", ""),
            "vram_gb": g.get("memoryInGb", 0),
            "community_price_hr": g.get("communityPrice") or "",
            "secure_price_hr": g.get("securePrice") or "",
            "community_spot_hr": g.get("communitySpotPrice") or "",
            "secure_spot_hr": g.get("secureSpotPrice") or "",
            "min_bid_price": lowest.get("minimumBidPrice") or "",
            "on_demand_price": lowest.get("uninterruptablePrice") or "",
            "stock_status": lowest.get("stockStatus", "Unknown"),
            "max_gpus_community": g.get("maxGpuCountCommunityCloud", 0),
            "max_gpus_secure": g.get("maxGpuCountSecureCloud", 0),
            "community_available": g.get("communityCloud", False),
            "secure_available": g.get("secureCloud", False),
        })

    print(f"  ✅ {len(rows)} GPU types collected")
    return rows


# ── CSV Writer ────────────────────────────────────────────────

def append_csv(filepath: Path, rows: list[dict]):
    """Append rows to CSV, creating file with headers if needed."""
    if not rows:
        return

    file_exists = filepath.exists() and filepath.stat().st_size > 0
    fieldnames = list(rows[0].keys())

    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"  💾 Wrote {len(rows)} rows to {filepath.name}")


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  GPU Market Intel Scraper")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Vast.ai
    if VAST_API_KEY:
        vast_rows = fetch_vastai()
        append_csv(VAST_CSV, vast_rows)
    else:
        print("⚠️  VAST_API_KEY not set — skipping Vast.ai")

    # RunPod
    if RUNPOD_API_KEY:
        runpod_rows = fetch_runpod()
        append_csv(RUNPOD_CSV, runpod_rows)
    else:
        print("⚠️  RUNPOD_API_KEY not set — skipping RunPod")

    print("\n✅ Scrape complete")


if __name__ == "__main__":
    main()
