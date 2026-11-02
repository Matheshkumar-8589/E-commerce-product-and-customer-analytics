#!/usr/bin/env python3
"""Download the real UCI 'Online Retail II' dataset and convert it to the project schema.

Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii  (CC BY 4.0)
Also on Kaggle: https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci
"""
import argparse, io, os, zipfile
import pandas as pd
import requests

URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
KEYWORDS = {
    "Home & Kitchen": ["MUG", "CUP", "PLATE", "BOWL", "CANDLE", "LANTERN", "FRAME", "CUSHION", "JAR", "TEAPOT", "KITCHEN", "CAKE"],
    "Fashion & Accessories": ["BAG", "SCARF", "HAT", "NECKLACE", "BRACELET", "EARRING", "JEWEL"],
    "Toys & Games": ["TOY", "GAME", "DOLL", "PUZZLE", "SPACEBOY", "DINOSAUR"],
    "Stationery & Books": ["CARD", "NOTEBOOK", "PEN", "PENCIL", "WRAP", "BOOK", "PAPER"],
    "Garden & Outdoor": ["GARDEN", "PLANT", "BIRD", "FLOWER", "POT"],
}


def categorise(name):
    n = str(name).upper()
    for cat, keys in KEYWORDS.items():
        if any(k in n for k in keys):
            return cat
    return "Gifts & Others"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/ecommerce_raw.csv")
    a = ap.parse_args()
    print("Downloading (~45 MB)...")
    z = zipfile.ZipFile(io.BytesIO(requests.get(URL, timeout=300).content))
    xlsx = [n for n in z.namelist() if n.endswith(".xlsx")][0]
    sheets = pd.read_excel(z.open(xlsx), sheet_name=None)          # two sheets: 2009-10, 2010-11
    raw = pd.concat(sheets.values(), ignore_index=True)
    df = pd.DataFrame({
        "order_id": raw["Invoice"].astype(str), "customer_id": raw["Customer ID"].astype("Int64"),
        "product_id": raw["StockCode"].astype(str), "product_name": raw["Description"],
        "category": raw["Description"].map(categorise), "quantity": raw["Quantity"],
        "unit_price": raw["Price"], "order_timestamp": pd.to_datetime(raw["InvoiceDate"]).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "region": raw["Country"]})
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    df.to_csv(a.out, index=False)
    print(f"Wrote {len(df):,} rows -> {a.out}")
