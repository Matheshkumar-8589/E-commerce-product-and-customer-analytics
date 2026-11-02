#!/usr/bin/env python3
"""Generate a large synthetic e-commerce dataset (with deliberately dirty rows).

Schema: order_id, customer_id, product_id, product_name, category, quantity,
        unit_price, order_timestamp, region
"""
import argparse, os
import numpy as np
import pandas as pd

CATEGORIES = {"Electronics": (40, 900), "Fashion": (10, 150), "Home & Kitchen": (8, 200),
              "Beauty": (5, 80), "Books": (4, 40), "Sports": (10, 250),
              "Toys": (5, 70), "Grocery": (2, 30)}
REGIONS = ["North India", "South India", "East India", "West India",
           "Central India", "UK", "USA", "UAE"]


def generate(rows=500_000, seed=42):
    rng = np.random.default_rng(seed)
    n_prod = 400
    cats = list(CATEGORIES)
    prod_cat = rng.choice(cats, n_prod, p=[.14, .20, .16, .10, .10, .12, .08, .10])
    prices = np.array([round(rng.uniform(*CATEGORIES[c]), 2) for c in prod_cat])
    pop = np.minimum(rng.zipf(1.6, n_prod), 50).astype(float)
    pop /= pop.sum()

    n_cust = max(1000, rows // 40)
    cust_region = rng.choice(REGIONS, n_cust, p=[.2, .22, .1, .18, .06, .08, .1, .06])
    cw = rng.pareto(1.5, n_cust) + 1
    cw /= cw.sum()

    n_orders = max(rows // 3, 1)
    order_cust = rng.choice(n_cust, n_orders, p=cw)
    days = pd.date_range("2023-01-01", "2025-12-31")
    season = {10: 1.3, 11: 1.6, 12: 1.8}
    w = np.array([season.get(m, 1.0) for m in days.month]) * (1 + 0.25 * (days.year.to_numpy() - 2023))
    order_day = rng.choice(len(days), n_orders, p=w / w.sum())
    order_ts = days[order_day] + pd.to_timedelta(rng.integers(0, 86400, n_orders), unit="s")

    lines = rng.integers(1, 6, n_orders)
    idx = np.repeat(np.arange(n_orders), lines)
    pid = rng.choice(n_prod, len(idx), p=pop)
    qty = rng.choice([1, 2, 3, 4, 5, 10], len(idx), p=[.45, .25, .12, .08, .07, .03])

    df = pd.DataFrame({
        "order_id": np.char.add("ORD", (100000 + idx).astype(str)),
        "customer_id": pd.array(10000 + order_cust[idx], dtype="Int64"),
        "product_id": np.char.add("P", np.char.zfill(pid.astype(str), 4)),
        "product_name": [f"{prod_cat[p]} Item {p:03d}" for p in pid],
        "category": prod_cat[pid],
        "quantity": qty,
        "unit_price": prices[pid],
        "order_timestamp": order_ts[idx].strftime("%Y-%m-%d %H:%M:%S"),
        "region": cust_region[order_cust[idx]],
    })
    # ---- inject dirty data (Veracity) ----
    df.loc[df.sample(frac=.01, random_state=1).index, "customer_id"] = pd.NA
    df.loc[df.sample(frac=.005, random_state=2).index, "unit_price"] = np.nan
    df.loc[df.sample(frac=.01, random_state=3).index, "category"] = None
    df.loc[df.sample(frac=.005, random_state=4).index, "quantity"] = -1
    dup = df.sample(frac=.02, random_state=5)
    return pd.concat([df, dup], ignore_index=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--out", default="data/raw/ecommerce_raw.csv")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    out = generate(a.rows, a.seed)
    out.to_csv(a.out, index=False)
    print(f"Wrote {len(out):,} rows -> {a.out}")
