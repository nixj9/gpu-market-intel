# 🖥️ GPU Market Intelligence Dashboard

Automated hourly scraper + interactive dashboard tracking GPU spot pricing and availability across **Vast.ai** and **RunPod**.

Built for [The Compute Brief](/) newsletter pipeline.

## Architecture

```
GitHub Actions (hourly cron)
    → scraper/scrape.py
    → commits to data/*.csv
    → GitHub Pages serves index.html
    → Dashboard reads CSVs client-side
```

**Cost: $0.** Runs entirely on GitHub's free tier.

---

## Quick Setup (15 minutes)

### 1. Create the repo

```bash
# Clone or fork this repo, or create fresh and copy files in
git init gpu-market-intel
cd gpu-market-intel
# Copy all files from this template into the repo
git add .
git commit -m "initial setup"
git remote add origin https://github.com/YOUR_USERNAME/gpu-market-intel.git
git push -u origin main
```

### 2. Add API keys as GitHub Secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

| Secret Name     | Value                        |
|-----------------|------------------------------|
| `VAST_API_KEY`  | Your Vast.ai read-only key   |
| `RUNPOD_API_KEY`| Your RunPod read-only key    |

### 3. Enable GitHub Pages

Go to repo → **Settings** → **Pages**:
- Source: **Deploy from a branch**
- Branch: **main** / **(root)**
- Click **Save**

Your dashboard will be live at: `https://YOUR_USERNAME.github.io/gpu-market-intel/`

### 4. Run the first scrape

Go to repo → **Actions** → **GPU Market Intel Scraper** → **Run workflow** (manual trigger)

This populates `data/vast_history.csv` and `data/runpod_history.csv`. After this, the cron runs automatically every hour.

---

## Files

```
├── .github/workflows/
│   └── scrape.yml          # Hourly GitHub Actions cron job
├── scraper/
│   └── scrape.py           # Python scraper (Vast.ai + RunPod APIs)
├── data/
│   ├── vast_history.csv    # Appended hourly (auto-generated)
│   └── runpod_history.csv  # Appended hourly (auto-generated)
├── index.html              # React dashboard (GitHub Pages)
└── README.md
```

## Dashboard Features

- **GPU SKU filter chips** — toggle individual models on/off
- **Platform filter** — Vast.ai only, RunPod only, or both
- **Time range** — 24h, 7d, 30d, or all time
- **Price history charts** — median spot pricing over time per GPU
- **Sortable pricing tables** — current snapshot with min/median/max
- **Availability tracking** — offer counts, stock status, regional distribution

## Customization

### Track different GPUs

Edit the `TARGET_GPUS` list in `scraper/scrape.py`:

```python
TARGET_GPUS = [
    "H100", "H100_SXM", "H200",
    "A100_SXM4", "A100_PCIE",
    "L40S", "RTX_4090",
    # Add more as needed
]
```

### Change scrape frequency

Edit `.github/workflows/scrape.yml` cron schedule:

```yaml
schedule:
  - cron: '15 * * * *'     # Every hour (default)
  # - cron: '*/30 * * * *' # Every 30 minutes
  # - cron: '0 */6 * * *'  # Every 6 hours
```

> GitHub Actions free tier: 2,000 minutes/month. Hourly runs ≈ 730 min/month (well within limits).

### Data retention

The CSV files grow over time (~1KB per scrape). After 6 months of hourly scraping, expect ~4MB total. If needed, add a cleanup step or archive old data periodically.

---

## Troubleshooting

**Scraper returns no data:**
- Check API keys in GitHub Secrets (exact names: `VAST_API_KEY`, `RUNPOD_API_KEY`)
- Run the Action manually and check the logs

**Dashboard shows "No data files found":**
- The scraper needs to run at least once first
- Check that `data/` directory has CSV files committed

**GitHub Actions not running on schedule:**
- Cron jobs only run on the default branch (main/master)
- GitHub may delay cron runs by a few minutes during high load
- If the repo has no activity for 60 days, Actions may be paused

---

## License

MIT — built for The Compute Brief newsletter.
