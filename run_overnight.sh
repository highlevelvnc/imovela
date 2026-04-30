#!/usr/bin/env bash
# ─── Imovela — overnight pipeline ────────────────────────────────────────────
# Runs the full capture + post-processing chain in one shot.
# Designed to be triggered manually or via cron at e.g. 03:30 daily.
#
# Total runtime on a fresh DB: ~3-4h.  After delta-cache primes (run #2+):
# ~30-60 min.
#
# Usage:
#   ./run_overnight.sh            # foreground (you watch the log)
#   nohup ./run_overnight.sh > logs/run_$(date +%H%M).log 2>&1 &
#                                  # background, safe to close terminal
#
# Stop a running run:
#   pkill -f "main.py run"

set -e

cd "$(dirname "$0")"
source venv/bin/activate

mkdir -p logs

echo "=== Run started: $(date) ==="

# Daily snapshot first — cheap insurance.
python main.py backup --keep 14

# Full pipeline (scrape + process + score + alerts + cross-match + websites)
python main.py run

# Drain any raw rows that didn't fit in the first pass
for i in 1 2 3 4 5 6 7 8; do
    echo "--- post-process batch $i ---"
    python main.py process --limit 2000 || break
done

# Re-score after the merges
python main.py score

# ML model + classification
python main.py train-owner-classifier
python main.py reclassify-owners --threshold 0.85

# Signals + enrichment
python main.py detect-price-drops
python main.py enrich-sellers
python main.py enrich-websites --max-agencies 100
python main.py geocode-leads --limit 2000

# Photo dedup
echo "--- photo dedup ---"
python main.py hash-images --limit 1500
python main.py dedup-photos --threshold 5

# Maintenance
python main.py archive-stale --days 60
python main.py sweep-dropped --limit 200

# Tags + search index
python main.py tag-amenities --limit 2000
python main.py rebuild-fts

# Final score pass + reports
python main.py score
python main.py trend-report
python main.py export-contacts --format both --score-min 30
python main.py export-contacts --format both --score-min 50
python main.py daily-digest --top 10 --score-min 60

echo "=== Run complete: $(date) ==="
