import pandas as pd
from sqlalchemy import create_engine, text
from config import load_settings

RAW_TABLES = [
    "dim_supplier",
    "dim_sales_channel",
    "dim_product",
    "dim_customer",
    "dim_campaign",
    "dim_page",
    "dim_warehouse",
    "dim_device",
    "fact_orders",
    "fact_shipments",
    "fact_support",
    "fact_product_reviews",
    "fact_campaign_performance",
    "fact_web_events",
    "fact_inventory_snapshot",
    "fact_manufacturing_throughput",
    "fact_device_telemetry",
    "fact_firmware_updates",
    "fact_warranty_claims",
    "fact_demand_forecast_vs_actual",
]

def main():
    s = load_settings()
    engine = create_engine(
        f"postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.database}"
    )

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))

    for t in RAW_TABLES:
        fp = s.raw_csv_dir / f"{t}.csv"
        if not fp.exists():
            raise FileNotFoundError(f"Missing CSV: {fp}")

        df = pd.read_csv(fp)
        # Load raw as-is; pandas will infer types, which is ok for raw.
        # If you want everything as text, you can force dtype=str for maximum chaos tolerance.
        df.to_sql(t, engine, schema="raw", if_exists="replace", index=False)

        print(f"Loaded raw.{t}: {len(df):,} rows")

if __name__ == "__main__":
    main()
