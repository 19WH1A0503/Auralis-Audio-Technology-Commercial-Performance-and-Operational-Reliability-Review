import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import load_settings
from sqlalchemy import create_engine, text
def main():
    s = load_settings()
    engine = create_engine(
        f"postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.database}"
    )

    # Load core silver tables
    orders = pd.read_sql("SELECT * FROM silver.fact_orders", engine)
    ship   = pd.read_sql("SELECT * FROM silver.fact_shipments", engine)

    # Gold: order fulfillment (orders + shipments)
    # If multiple shipments per order, keep the latest actual_dt
    ship_agg = (ship
        .groupby("order_id_clean", dropna=False)
        .agg(
            ship_dt=("ship_dt","max"),
            promised_dt=("promised_dt","max"),
            actual_dt=("actual_dt","max"),
            shipping_cost_num=("shipping_cost_num","mean"),
            carrier=("carrier","last"),
            warehouse_id=("warehouse_id_clean","last"),
        )
        .reset_index()
        .rename(columns={"order_id_clean":"order_id_clean"})
    )

    gold_fulfillment = orders.merge(ship_agg, on="order_id_clean", how="left")

    gold_fulfillment["is_late"] = np.where(
        (gold_fulfillment["actual_dt"].notna()) &
        (gold_fulfillment["promised_dt"].notna()) &
        (gold_fulfillment["actual_dt"] > gold_fulfillment["promised_dt"]),
        1, 0
    )
    gold_fulfillment["days_late"] = np.where(
        (gold_fulfillment["actual_dt"].notna()) & (gold_fulfillment["promised_dt"].notna()),
        (gold_fulfillment["actual_dt"] - gold_fulfillment["promised_dt"]).dt.days,
        np.nan
    )

    # Revenue
    gold_fulfillment["revenue"] = (
        gold_fulfillment["units_sold_num"].fillna(0) * gold_fulfillment["net_unit_price_num"].fillna(0)
    )

    gold_fulfillment.to_sql("order_fulfillment", engine, schema="gold", if_exists="replace", index=False)

    # Gold: funnel_daily (aggregated from web events)
    # Gold: funnel_daily (session-based funnel from web events)
    web = pd.read_sql("SELECT * FROM silver.fact_web_events", engine)
    prod = pd.read_sql("SELECT product_id, product_type FROM silver.dim_product", engine)
    sc   = pd.read_sql("SELECT sales_channel_id, channel_name FROM silver.dim_sales_channel", engine)
    
    web = web.merge(prod, left_on="product_id_clean", right_on="product_id", how="left")
    web = web.merge(sc, left_on="sales_channel_id_clean", right_on="sales_channel_id", how="left")
    
    web["event_ts_dt"] = pd.to_datetime(web["event_ts_dt"])
    web["event_date"] = web["event_ts_dt"].dt.date
    
    # one row per session-product-day
    grain = [
        "event_date",
        "sales_channel_id_clean",
        "channel_name",
        "product_type",
        "session_id_clean",
        "product_id_clean"
    ]
    
    stage_flags = (
        web.groupby(grain, dropna=False)
           .agg(
               product_view=("event_type", lambda x: int((x == "product_view").any())),
               add_to_cart=("event_type", lambda x: int((x == "add_to_cart").any())),
               purchase=("event_type", lambda x: int((x == "purchase").any())),
           )
           .reset_index()
    )
    
    # strict funnel logic
    stage_flags["add_to_cart"] = np.where(
        stage_flags["product_view"] == 1,
        stage_flags["add_to_cart"],
        0
    )
    
    stage_flags["purchase"] = np.where(
        stage_flags["add_to_cart"] == 1,
        stage_flags["purchase"],
        0
    )
    
    funnel = (
        stage_flags.groupby(
            ["event_date", "sales_channel_id_clean", "channel_name", "product_type"],
            dropna=False
        )
        .agg(
            product_view=("product_view", "sum"),
            add_to_cart=("add_to_cart", "sum"),
            purchase=("purchase", "sum")
        )
        .reset_index()
        .rename(columns={"sales_channel_id_clean": "sales_channel_id"})
    )
    
    funnel["pv_to_atc_rate"] = np.where(
        funnel["product_view"] > 0,
        funnel["add_to_cart"] / funnel["product_view"],
        np.nan
    )
    
    funnel["atc_to_purchase_rate"] = np.where(
        funnel["add_to_cart"] > 0,
        funnel["purchase"] / funnel["add_to_cart"],
        np.nan
    )
    
    funnel.to_sql("funnel_daily", engine, schema="gold", if_exists="replace", index=False)
    
       
    # Gold: device_health_daily
    tel = pd.read_sql("SELECT * FROM silver.fact_device_telemetry", engine)
    dev = pd.read_sql("SELECT device_id, product_id FROM silver.dim_device", engine)
    prod = pd.read_sql("SELECT product_id, product_type FROM silver.dim_product", engine)
    
    tel = tel.merge(dev, left_on="device_id_clean", right_on="device_id", how="left")
    tel = tel.merge(prod, on="product_id", how="left")
    
    tel["telemetry_date"] = tel["telemetry_ts_dt"].dt.date
    
    health = (tel
        .groupby(["telemetry_date","product_type"], dropna=False)
        .agg(
            avg_disconnects=("bt_disconnects", "mean"),
            crash_rate=("crash_flag", lambda x: np.nanmean(x.astype(float)) if len(x) else np.nan),
            avg_temp_c=("temp_c_num","mean"),
            rows=("telemetry_id","count"),
        )
        .reset_index()
    )
    
    health.to_sql("device_health_daily", engine, schema="gold", if_exists="replace", index=False)
    
    print("✅ Built gold.* tables: order_fulfillment, funnel_daily, device_health_daily")

if __name__ == "__main__":
    main()


