import re
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from config import load_settings

def to_null(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return np.nan
    return s

def clean_id(s: pd.Series) -> pd.Series:
    return s.apply(to_null)

def clean_device_id(s: pd.Series) -> pd.Series:
    return s.apply(to_null).str.strip().str.lower()

def parse_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="mixed")

def to_num(s: pd.Series, strip_dollar=False, strip_c=False) -> pd.Series:
    ss = s.astype(str).str.strip()
    ss = ss.replace({"nan": np.nan, "None": np.nan, "null": np.nan, "": np.nan})
    if strip_dollar:
        ss = ss.str.replace("$", "", regex=False)
    if strip_c:
        ss = ss.str.replace("C", "", regex=False)
    return pd.to_numeric(ss, errors="coerce")

def parse_fail_flag(raw: pd.Series, parsed: pd.Series) -> pd.Series:
    return ((raw.notna()) & (parsed.isna())).astype(int)

def main():
    s = load_settings()
    engine = create_engine(
        f"postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.database}"
    )

    # ---------- DIM SILVER ----------
    dim_customer = pd.read_sql("SELECT * FROM raw.dim_customer", engine)
    dim_customer["customer_id_clean"] = clean_id(dim_customer["customer_id"])
    dim_customer["signup_dt"] = parse_dt(dim_customer["signup_date"])
    dim_customer["is_signup_date_bad"] = parse_fail_flag(dim_customer["signup_date"], dim_customer["signup_dt"])
    dim_customer.to_sql("dim_customer", engine, schema="silver", if_exists="replace", index=False)

    dim_product = pd.read_sql("SELECT * FROM raw.dim_product", engine)
    dim_product["product_id_clean"] = clean_id(dim_product["product_id"])
    dim_product["primary_supplier_id_clean"] = clean_id(dim_product["primary_supplier_id"])
    dim_product["launch_dt"] = parse_dt(dim_product["launch_date"])
    dim_product["is_launch_date_bad"] = parse_fail_flag(dim_product["launch_date"], dim_product["launch_dt"])
    dim_product["msrp_num"] = to_num(dim_product["msrp"])
    dim_product["is_msrp_bad"] = parse_fail_flag(dim_product["msrp"], dim_product["msrp_num"])
    dim_product.to_sql("dim_product", engine, schema="silver", if_exists="replace", index=False)

    dim_sales_channel = pd.read_sql("SELECT * FROM raw.dim_sales_channel", engine)
    dim_sales_channel["sales_channel_id_clean"] = clean_id(dim_sales_channel["sales_channel_id"])
    dim_sales_channel["launch_dt"] = parse_dt(dim_sales_channel["launch_date"])
    dim_sales_channel["is_launch_date_bad"] = parse_fail_flag(dim_sales_channel["launch_date"], dim_sales_channel["launch_dt"])
    dim_sales_channel.to_sql("dim_sales_channel", engine, schema="silver", if_exists="replace", index=False)

    dim_device = pd.read_sql("SELECT * FROM raw.dim_device", engine)
    dim_device["device_id_clean"] = clean_device_id(dim_device["device_id"])
    dim_device["product_id_clean"] = clean_id(dim_device["product_id"])
    dim_device["activated_dt"] = parse_dt(dim_device["activated_date"])
    dim_device["is_activated_date_bad"] = parse_fail_flag(dim_device["activated_date"], dim_device["activated_dt"])
    dim_device.to_sql("dim_device", engine, schema="silver", if_exists="replace", index=False)

    # other dims: light pass-through
    for name in ["dim_supplier","dim_campaign","dim_page","dim_warehouse"]:
        df = pd.read_sql(f"SELECT * FROM raw.{name}", engine)
        df.to_sql(name, engine, schema="silver", if_exists="replace", index=False)

    # ---------- FACT SILVER ----------
    fact_orders = pd.read_sql("SELECT * FROM raw.fact_orders", engine)
    for c in ["order_id","customer_id","product_id","sales_channel_id","attributed_campaign_id"]:
        if c in fact_orders.columns:
            fact_orders[f"{c}_clean"] = clean_id(fact_orders[c])

    fact_orders["order_dt"] = parse_dt(fact_orders["order_date"])
    fact_orders["delivery_dt"] = parse_dt(fact_orders["delivery_date"])
    fact_orders["return_dt"] = parse_dt(fact_orders.get("return_date", pd.Series([np.nan]*len(fact_orders))))

    fact_orders["is_order_date_bad"] = parse_fail_flag(fact_orders["order_date"], fact_orders["order_dt"])
    fact_orders["is_delivery_date_bad"] = parse_fail_flag(fact_orders["delivery_date"], fact_orders["delivery_dt"])

    for col in ["units_sold","unit_price","net_unit_price","discount_pct","msrp"]:
        if col in fact_orders.columns:
            fact_orders[f"{col}_num"] = to_num(fact_orders[col])
            fact_orders[f"is_{col}_bad"] = parse_fail_flag(fact_orders[col], fact_orders[f"{col}_num"])

    fact_orders.to_sql("fact_orders", engine, schema="silver", if_exists="replace", index=False)

    fact_ship = pd.read_sql("SELECT * FROM raw.fact_shipments", engine)
    fact_ship["order_id_clean"] = clean_id(fact_ship["order_id"])
    fact_ship["warehouse_id_clean"] = clean_id(fact_ship.get("warehouse_id", pd.Series([np.nan]*len(fact_ship))))
    fact_ship["ship_dt"] = parse_dt(fact_ship["ship_date"])
    fact_ship["promised_dt"] = parse_dt(fact_ship["promised_delivery_date"])
    fact_ship["actual_dt"] = parse_dt(fact_ship["actual_delivery_date"])
    fact_ship["shipping_cost_num"] = to_num(fact_ship.get("shipping_cost", pd.Series([np.nan]*len(fact_ship))), strip_dollar=True)
    fact_ship["is_order_id_format_bad"] = fact_ship["order_id"].astype(str).apply(lambda x: 1 if ("ORD" in x and "-" not in x) else 0)
    fact_ship.to_sql("fact_shipments", engine, schema="silver", if_exists="replace", index=False)

    fact_web = pd.read_sql("SELECT * FROM raw.fact_web_events", engine)
    for c in ["customer_id","order_id","product_id","campaign_id","sales_channel_id"]:
        if c in fact_web.columns:
            fact_web[f"{c}_clean"] = clean_id(fact_web[c])
    fact_web["event_ts_dt"] = parse_dt(fact_web["event_ts"])
    fact_web["is_event_ts_bad"] = parse_fail_flag(fact_web["event_ts"], fact_web["event_ts_dt"])
    fact_web["event_value_num"] = to_num(fact_web.get("event_value", pd.Series([np.nan]*len(fact_web))))
    fact_web["is_customer_id_prefix_bad"] = fact_web["customer_id_clean"].astype(str).str.startswith("CST-").astype(int)
    fact_web.to_sql("fact_web_events", engine, schema="silver", if_exists="replace", index=False)

    fact_tel = pd.read_sql("SELECT * FROM raw.fact_device_telemetry", engine)
    fact_tel["device_id_clean"] = clean_device_id(fact_tel["device_id"])
    fact_tel["telemetry_ts_dt"] = parse_dt(fact_tel["telemetry_ts"])
    fact_tel["temp_c_num"] = to_num(fact_tel.get("temp_c", pd.Series([np.nan]*len(fact_tel))), strip_c=True)
    fact_tel.to_sql("fact_device_telemetry", engine, schema="silver", if_exists="replace", index=False)

    fact_fw = pd.read_sql("SELECT * FROM raw.fact_firmware_updates", engine)
    fact_fw["device_id_clean"] = clean_device_id(fact_fw.get("device_id", pd.Series([np.nan]*len(fact_fw))))
    fact_fw["update_ts_dt"] = parse_dt(fact_fw["update_ts"])
    fact_fw.to_sql("fact_firmware_updates", engine, schema="silver", if_exists="replace", index=False)

    fact_warr = pd.read_sql("SELECT * FROM raw.fact_warranty_claims", engine)
    fact_warr["order_id_clean"] = clean_id(fact_warr["order_id"])
    fact_warr["device_id_clean"] = clean_device_id(fact_warr.get("device_id", pd.Series([np.nan]*len(fact_warr))))
    fact_warr["claim_dt"] = parse_dt(fact_warr["claim_date"])
    fact_warr["is_order_prefix_bad"] = fact_warr["order_id_clean"].astype(str).str.startswith("OR-").astype(int)
    fact_warr.to_sql("fact_warranty_claims", engine, schema="silver", if_exists="replace", index=False)

    # pass-through remaining facts
    for name in ["fact_support","fact_product_reviews","fact_campaign_performance",
                 "fact_inventory_snapshot","fact_manufacturing_throughput","fact_demand_forecast_vs_actual"]:
        df = pd.read_sql(f"SELECT * FROM raw.{name}", engine)
        df.to_sql(name, engine, schema="silver", if_exists="replace", index=False)

    print("✅ Built silver.* tables")

if __name__ == "__main__":
    main()