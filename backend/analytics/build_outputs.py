#!/usr/bin/env python3
"""Python analytics: turn Spark exports into (1) charts + text report,
(2) the web dashboard data file, (3) Power BI-ready CSV files."""
import argparse, json, os, shutil
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def to_table(df):
    return {"cols": list(df.columns), "rows": json.loads(df.to_json(orient="values"))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exports", default="backend/outputs")
    ap.add_argument("--web", default="frontend/web_dashboard/data")
    ap.add_argument("--powerbi", default="frontend/powerbi/data")
    a = ap.parse_args()
    E = lambda n: pd.read_csv(os.path.join(a.exports, n))
    kpis = json.load(open(os.path.join(a.exports, "kpis.json")))
    quality = json.load(open(os.path.join(a.exports, "data_quality.json")))
    monthly, yearly, cats = E("monthly.csv"), E("yearly.csv"), E("categories.csv")
    top, regions = E("top_products.csv"), E("regions.csv")
    cube, ocube, segs = E("cube.csv"), E("order_cube.csv"), E("segment_summary.csv")
    rfm = E("customer_rfm.csv")

    # ---- 1. charts ----
    cd = os.path.join(a.exports, "charts"); os.makedirs(cd, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 4)); ax.plot(monthly.year_month, monthly.revenue, marker="o", color="#0f5257")
    ax.set_title("Monthly revenue"); ax.set_xticks(range(0, len(monthly), 3)); ax.set_xticklabels(monthly.year_month[::3], rotation=45)
    fig.tight_layout(); fig.savefig(f"{cd}/monthly_revenue.png", dpi=120); plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 4)); ax.barh(cats.category[::-1], cats.revenue[::-1], color="#e0a526")
    ax.set_title("Revenue by category"); fig.tight_layout(); fig.savefig(f"{cd}/category_revenue.png", dpi=120); plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 4)); t = top.head(10)
    ax.barh(t.product_name[::-1], t.revenue[::-1], color="#0f5257"); ax.set_title("Top 10 products by revenue")
    fig.tight_layout(); fig.savefig(f"{cd}/top_products.png", dpi=120); plt.close(fig)
    s = rfm.segment.value_counts()
    fig, ax = plt.subplots(figsize=(5, 5)); ax.pie(s.values, labels=s.index, autopct="%1.0f%%"); ax.set_title("Customer segments")
    fig.tight_layout(); fig.savefig(f"{cd}/segments.png", dpi=120); plt.close(fig)

    # ---- 2. text report ----
    seg_rev = rfm.groupby("segment").monetary.sum().sort_values(ascending=False)
    best_m = monthly.loc[monthly.revenue.idxmax()]
    lines = ["E-COMMERCE BIG DATA ANALYTICS - SUMMARY REPORT", "=" * 50,
             f"Total revenue        : {kpis['total_revenue']:,.2f}", f"Total orders         : {kpis['total_orders']:,}",
             f"Total customers      : {kpis['total_customers']:,}", f"Average order value  : {kpis['avg_order_value']:,.2f}",
             f"Best month           : {best_m.year_month} ({best_m.revenue:,.2f})",
             f"Top category         : {cats.iloc[0].category} ({cats.iloc[0].revenue:,.2f})",
             f"Top region           : {regions.iloc[0].region} ({regions.iloc[0].revenue:,.2f})",
             f"Top product          : {top.iloc[0].product_name} ({top.iloc[0].revenue:,.2f})", "",
             "Yearly revenue:", yearly.to_string(index=False), "",
             "Revenue by segment:", seg_rev.round(2).to_string(), "",
             "Data quality (Veracity):", json.dumps(quality, indent=2)]
    open(os.path.join(a.exports, "summary_report.txt"), "w").write("\n".join(lines))

    # ---- 3. web dashboard data ----
    os.makedirs(a.web, exist_ok=True)
    # compact cubes keep the browser fast: category-level cube + product cube for the top 30 products
    keys = ["year_month", "category", "region", "segment"]
    small = cube.groupby(keys, as_index=False)[["revenue", "units", "lines"]].sum()
    top100 = cube.groupby("product_name").revenue.sum().nlargest(30).index
    prod = cube[cube.product_name.isin(top100)].groupby(keys + ["product_name"], as_index=False)[["revenue", "units", "lines"]].sum()
    payload = {"kpis": kpis, "quality": quality, "cube": to_table(small), "products": to_table(prod),
               "order_cube": to_table(ocube), "segments": to_table(segs)}
    open(os.path.join(a.web, "data.js"), "w").write("window.DASH = " + json.dumps(payload) + ";")

    # ---- 4. Power BI files ----
    os.makedirs(a.powerbi, exist_ok=True)
    cube["month_start"] = cube.year_month + "-01"; ocube["month_start"] = ocube.year_month + "-01"
    monthly["month_start"] = monthly.year_month + "-01"
    for name, df in {"fact_sales_cube": cube, "fact_orders": ocube, "monthly_sales": monthly, "yearly_sales": yearly,
                     "top_products": top, "category_performance": cats, "region_performance": regions,
                     "customer_segments": rfm, "segment_summary": segs}.items():
        df.to_csv(os.path.join(a.powerbi, f"{name}.csv"), index=False)
    print("Charts, report, dashboard data and Power BI files written.")


if __name__ == "__main__":
    main()
