#!/usr/bin/env python3
"""
Synthetic card-transaction generator.

WHY THE DATA LOOKS LIKE THIS
    The models in this project are only interesting if the data has the same
    awkward shape as the real thing. At American Express the authorization and
    settlement streams arrive separately and do NOT line up one-to-one:

      * a settlement lands 0-5 days AFTER its authorization, so yesterday's
        totals keep changing unless the model pins an as-of window
      * the settled amount can differ from the authorized amount (tips,
        partial captures, currency rounding)
      * some authorizations never settle at all (abandoned, reversed)
      * merchant attributes change over time, which is why the dimension
        needs SCD2 rather than an overwrite

    A generator that emitted perfectly matched pairs would let a naive JOIN
    look correct, which would defeat the point of the project.
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42                      # deterministic: `dbt build` must be reproducible
DAYS = 45
MERCHANTS = 60
TXNS_PER_DAY = (400, 700)

# Share of authorizations that never settle. Real reversal rates sit low
# single digits; 4% keeps the unsettled bucket visible in the marts.
NEVER_SETTLES = 0.04
# Settlement lag in days. Most land next-day; the tail is what breaks naive
# incremental models that only look at "today".
# The 7/9-day tail is deliberate: it is outside the agreed window and must
# show up as a LATE_SETTLEMENT exception. Without it that branch is dead code.
LAG_WEIGHTS = {0: 0.15, 1: 0.53, 2: 0.15, 3: 0.08, 4: 0.05, 5: 0.02,
               7: 0.01, 9: 0.01}

MCC = [
    ("5411", "Grocery Stores"), ("5812", "Restaurants"),
    ("5541", "Service Stations"), ("4511", "Airlines"),
    ("7011", "Lodging"), ("5732", "Electronics"),
    ("5999", "Misc Retail"), ("4899", "Cable & Streaming"),
]
REGIONS = ["US-WEST", "US-EAST", "US-CENTRAL", "US-SOUTH"]

root = Path(__file__).resolve().parent.parent
seeds = root / "seeds"
seeds.mkdir(exist_ok=True)

rnd = random.Random(SEED)
end = date(2026, 9, 15)
start = end - timedelta(days=DAYS - 1)


def pick_lag() -> int:
    r, acc = rnd.random(), 0.0
    for lag, w in LAG_WEIGHTS.items():
        acc += w
        if r <= acc:
            return lag
    return 1


# --------------------------------------------------------------- merchants
# Two rows for a handful of merchants: an attribute changes partway through
# the window. That is what the SCD2 snapshot has to capture - an overwrite
# would silently rewrite history for every fact that joined to the old row.
merchant_rows = []
changed = set(rnd.sample(range(1, MERCHANTS + 1), 8))
for mid in range(1, MERCHANTS + 1):
    mcc, desc = rnd.choice(MCC)
    merchant_rows.append({
        "merchant_id": mid,
        "merchant_name": f"Merchant {mid:03d}",
        "mcc_code": mcc,
        "mcc_description": desc,
        "region": rnd.choice(REGIONS),
        "risk_tier": rnd.choice(["LOW", "LOW", "MEDIUM", "HIGH"]),
        "updated_at": f"{start} 00:00:00",
    })

with open(seeds / "raw_merchants.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(merchant_rows[0].keys()))
    w.writeheader()
    w.writerows(merchant_rows)

# The changed-attribute version, applied later in the window. The snapshot is
# run against this file on a second pass (see README) to produce SCD2 history.
for r in merchant_rows:
    if r["merchant_id"] in changed:
        r["risk_tier"] = "HIGH" if r["risk_tier"] != "HIGH" else "MEDIUM"
        r["region"] = rnd.choice(REGIONS)
        r["updated_at"] = f"{end - timedelta(days=5)} 00:00:00"

with open(seeds / "raw_merchants_v2.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(merchant_rows[0].keys()))
    w.writeheader()
    w.writerows(merchant_rows)

# --------------------------------------------------- authorizations + settlements
auths, setts = [], []
auth_id = 0
for d in (start + timedelta(days=i) for i in range(DAYS)):
    for _ in range(rnd.randint(*TXNS_PER_DAY)):
        auth_id += 1
        mid = rnd.randint(1, MERCHANTS)
        amount = round(rnd.lognormvariate(3.2, 0.9), 2)
        hour = rnd.randint(0, 23)
        auths.append({
            "auth_id": auth_id,
            "merchant_id": mid,
            "card_token": f"tok_{rnd.randint(1, 5000):05d}",
            "auth_amount": amount,
            "auth_ts": f"{d} {hour:02d}:{rnd.randint(0,59):02d}:00",
            "auth_date": str(d),
            "currency": "USD",
        })
        if rnd.random() < NEVER_SETTLES:
            continue
        lag = pick_lag()
        sd = d + timedelta(days=lag)
        if sd > end:                      # not yet settled as of the snapshot
            continue
        # Settled amount drifts from authorized: tips and partial captures.
        # 1.40 exceeds the 25% over-capture threshold; 1.18/1.20 are normal
        # tips and must NOT trip it. Both sides of the boundary are present
        # on purpose so the exception rule is actually tested.
        drift = rnd.choice([1.0, 1.0, 1.0, 1.18, 1.20, 1.40, 0.45, 0.97])
        setts.append({
            "settlement_id": f"s{auth_id}",
            "auth_id": auth_id,
            "merchant_id": mid,
            "settled_amount": round(amount * drift, 2),
            "settled_ts": f"{sd} {rnd.randint(0,23):02d}:{rnd.randint(0,59):02d}:00",
            "settled_date": str(sd),
            "currency": "USD",
        })

with open(seeds / "raw_authorizations.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(auths[0].keys()))
    w.writeheader()
    w.writerows(auths)

with open(seeds / "raw_settlements.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(setts[0].keys()))
    w.writeheader()
    w.writerows(setts)

unsettled = len(auths) - len(setts)
print(f"merchants      {len(merchant_rows)}  ({len(changed)} with a later attribute change)")
print(f"authorizations {len(auths):,}")
print(f"settlements    {len(setts):,}")
print(f"unsettled      {unsettled:,}  ({100*unsettled/len(auths):.1f}%)")
print(f"window         {start} .. {end}")
