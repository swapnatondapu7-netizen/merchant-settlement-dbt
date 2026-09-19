# merchant-settlement-dbt

A dbt project that models **card authorization vs settlement reconciliation** at
merchant-day grain — incremental models with a late-arrival window, an SCD2
merchant dimension, and the data tests that catch a broken join before anyone
downstream sees it.

I built this to express in dbt the transformation, DAG and testing work I do by
hand at American Express, where I work on the batch and streaming pipelines
behind roughly 1M card transactions and ~8 TB a day.

It runs on DuckDB, so `dbt build` works immediately after clone with no
warehouse credentials.

```bash
python -m venv .venv && ./.venv/bin/pip install dbt-duckdb
./.venv/bin/python scripts/generate_data.py     # synthetic auth + settlement streams
./.venv/bin/dbt build --profiles-dir .          # 33 models, snapshots and tests
```

---

## The problem this models

Authorization and settlement arrive as **two separate streams that do not line
up one-to-one**:

- a settlement lands **0–9 days after** its authorization
- the settled amount **differs** from the authorized amount (tips, partial captures)
- some authorizations **never settle** (reversed, abandoned)
- merchant attributes (risk tier, region) **change over time**

Each of those breaks a naive implementation in a way that produces no error —
just a wrong number. The project is organised around handling them explicitly.

## What the pipeline does

```
seeds (raw auth / settlement / merchant streams)
  └── staging/          cast + rename only, one model per source
        └── intermediate/int_auth_settlement_matched
              │           auth LEFT JOIN settlement, grain held at the auth
              ├── marts/fct_merchant_daily_volume      (incremental)
              └── marts/fct_settlement_exceptions      (what finance chases)
  └── snapshots/snap_merchants                          (SCD2 history)
```

### 1. The late-arrival window — the core of the project

A settlement can land days after its authorization, so **days that already
looked finished keep changing**. An incremental model filtered on
`auth_date = current_date` never revisits those days, and they under-report
forever with nothing in the logs.

`fct_merchant_daily_volume` instead reprocesses a trailing window and replaces
those days wholesale:

```sql
{{ config(materialized='incremental',
          unique_key=['merchant_id','auth_date'],
          incremental_strategy='delete+insert') }}

{% if is_incremental() %}
where auth_date >= (
    select coalesce(max(auth_date), '1900-01-01'::date)
             - interval '{{ var("settlement_lag_days") }} days'
    from {{ this }}
)
{% endif %}
```

**Verified, not asserted.** Injecting a settlement that arrives 4 days late and
re-running the model:

```
before:  merchant 6 on 2026-09-11 -> 4 open auths, settled $173.78
after:   merchant 6 on 2026-09-11 -> 3 open auths, settled $187.87
```

The model reached back and corrected a day it had already written.

### 2. LEFT JOIN, and why the grain is the authorization

An inner join silently drops the two populations the business cares about most:

| status | meaning | rows |
|---|---|---|
| `SETTLED` | matched | 22,564 |
| `PENDING` | not settled *yet*, still inside the window | ~578 |
| `UNSETTLED` | past the window, never settled | ~1,830 |

Both non-settled groups simply vanish under an inner join and merchant volume
comes out low with no error anywhere. Holding the grain on the authorization
side also prevents the classic fan-out bug — `int_auth_settlement_matched` has
a `unique` test on `auth_id` precisely to catch it.

### 3. SCD2 merchant dimension

Risk tier and region change. Overwriting the dimension silently re-attributes
historical facts to a merchant's *current* attributes, so last quarter's volume
by risk tier changes every time someone re-tiers a merchant — and a report run
twice gives two answers.

```
snap_merchants: 68 rows | 60 current | 8 historical

merchant 2  risk=LOW   region=US-WEST  valid 2026-08-02 -> 2026-09-10
merchant 2  risk=HIGH  region=US-WEST  valid 2026-09-10 -> CURRENT
```

### 4. Tests — and one that had to be corrected

**33 checks pass**: `unique`, `not_null`, `relationships` between the two
streams, `accepted_values` on status and exception enums, a
unique-combination test on the mart's grain, and a singular reconciliation
invariant.

The invariant is the interesting one. First version asserted that aggregate
settled volume never exceeds authorized by >30% — and it **failed on 2 of 2,699
merchant-days**, with 3 and 13 authorizations respectively. On a 3-transaction
day a single legitimate 1.4× over-capture moves the whole ratio. The signal was
small-sample noise, not a broken join.

Rather than loosen the threshold and weaken the test everywhere, I added a
volume floor and documented why:

```sql
where auth_count >= 20                      -- below this, one tip dominates
  and settled_amount > authorized_amount * 1.30
```

A test that fails on correct data trains people to ignore it. Low-volume days
are covered by `fct_settlement_exceptions` instead, which is a report rather
than a build-blocking assertion.

## Repo layout

```
models/staging/        stg_authorizations, stg_settlements, stg_merchants
models/intermediate/   int_auth_settlement_matched
models/marts/          fct_merchant_daily_volume, fct_settlement_exceptions
snapshots/             snap_merchants (SCD2)
tests/                 custom generic tests + the reconciliation invariant
scripts/               synthetic data generator
```

## Notes on the data

`scripts/generate_data.py` is seeded (deterministic) and deliberately emits
awkward data: a 7–9 day settlement tail *outside* the agreed window, drift
values on **both sides** of the 25% over-capture threshold (1.18/1.20 are
normal tips and must not trip it; 1.40 must), and merchants whose attributes
change mid-window. Data that matched perfectly would let a naive join look
correct, which would defeat the point.

## Stack

dbt 1.12 · DuckDB (swap `profiles.yml` for Snowflake/BigQuery — the SQL is
standard) · Python for data generation
