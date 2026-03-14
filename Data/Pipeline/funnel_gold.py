import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ["PGDATABASE"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def main():
    engine = get_engine()

    web = pd.read_sql("SELECT * FROM silver.fact_web_events", engine)
    prod = pd.read_sql("SELECT product_id, product_type FROM silver.dim_product", engine)
    sc = pd.read_sql("SELECT sales_channel_id, channel_name FROM silver.dim_sales_channel", engine)

    web = web.merge(prod, left_on="product_id_clean", right_on="product_id", how="left")
    web = web.merge(sc, left_on="sales_channel_id_clean", right_on="sales_channel_id", how="left")

    web["event_ts_dt"] = pd.to_datetime(web["event_ts_dt"])
    web["event_date"] = web["event_ts_dt"].dt.date

    funnel = (
        web.pivot_table(
            index=["event_date", "sales_channel_id_clean", "channel_name", "product_type"],
            columns="event_type",
            values="event_id",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={"sales_channel_id_clean": "sales_channel_id"})
    )

    funnel["pv_to_atc_rate"] = np.where(
        funnel.get("product_view", 0) > 0,
        funnel.get("add_to_cart", 0) / funnel.get("product_view", 0),
        np.nan,
    )

    funnel["atc_to_purchase_rate"] = np.where(
        funnel.get("add_to_cart", 0) > 0,
        funnel.get("purchase", 0) / funnel.get("add_to_cart", 0),
        np.nan,
    )

    funnel = funnel[
        [
            "event_date",
            "sales_channel_id",
            "channel_name",
            "product_type",
            "add_to_cart",
            "product_view",
            "pv_to_atc_rate",
            "atc_to_purchase_rate",
        ]
    ]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.funnel_daily"))

    funnel.to_sql("funnel_daily", engine, schema="gold", if_exists="append", index=False)
    print("✅ Rebuilt gold.funnel_daily")