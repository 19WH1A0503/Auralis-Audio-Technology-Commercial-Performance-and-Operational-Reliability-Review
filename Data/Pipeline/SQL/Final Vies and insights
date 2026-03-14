--Daily exec marts
CREATE OR REPLACE VIEW mart.kpi_daily AS
SELECT
  of.order_dt::date AS date,
  COUNT(DISTINCT of.order_id_clean) AS orders,
  SUM(of.revenue) AS revenue,
  AVG(CASE WHEN of.return_flag = 1 THEN 1 ELSE 0 END)::numeric(10,4) AS return_rate,
  AVG(CASE WHEN of.is_late = 0 THEN 1 ELSE 0 END)::numeric(10,4) AS on_time_rate,
  AVG(of.days_late)::numeric(10,2) AS avg_days_late
FROM gold.order_fulfillment of
WHERE of.order_dt IS NOT NULL
GROUP BY 1;

CREATE OR REPLACE VIEW mart.shipping_sla AS
SELECT
  COALESCE(of.carrier, 'UNKNOWN') AS carrier,
  COALESCE(of.warehouse_id, 'UNKNOWN') AS warehouse_id,
  COUNT(*) AS shipments,
  AVG(of.is_late)::numeric(10,4) AS late_rate,
  AVG(of.days_late)::numeric(10,2) AS avg_days_late,
  SUM(CASE WHEN of.is_late = 1 THEN 1 ELSE 0 END) AS late_shipments
FROM gold.order_fulfillment of
GROUP BY 1,2;

--Shipping SLA by carrier + warehouse
CREATE OR REPLACE VIEW mart.sales_mix AS
SELECT
  of.sales_channel_id_clean AS sales_channel_id,
  sc.channel_name,
  p.product_type,
  COUNT(DISTINCT of.order_id_clean) AS orders,
  SUM(of.revenue) AS revenue,
  AVG(CASE WHEN of.return_flag = 1 THEN 1 ELSE 0 END)::numeric(10,4) AS return_rate,
  AVG(of.is_late)::numeric(10,4) AS late_rate
FROM gold.order_fulfillment of
LEFT JOIN silver.dim_sales_channel sc
  ON of.sales_channel_id_clean = sc.sales_channel_id
LEFT JOIN silver.dim_product p
  ON of.product_id_clean = p.product_id
GROUP BY 1,2,3;

--Revenue + returns by channel and product type
CREATE OR REPLACE VIEW mart.sales_mix AS
SELECT
  of.sales_channel_id_clean AS sales_channel_id,
  sc.channel_name,
  p.product_type,
  COUNT(DISTINCT of.order_id_clean) AS orders,
  SUM(of.revenue) AS revenue,
  AVG(CASE WHEN of.return_flag = 1 THEN 1 ELSE 0 END)::numeric(10,4) AS return_rate,
  AVG(of.is_late)::numeric(10,4) AS late_rate
FROM gold.order_fulfillment of
LEFT JOIN silver.dim_sales_channel sc
  ON of.sales_channel_id_clean = sc.sales_channel_id
LEFT JOIN silver.dim_product p
  ON of.product_id_clean = p.product_id
GROUP BY 1,2,3;

--Marketing performance (ROAS/CTR/CVR)
CREATE OR REPLACE VIEW mart.marketing_performance AS
SELECT
  cp.campaign_id,
  c.campaign_name,
  c.objective,
  cp.region,
  SUM(cp.impressions) AS impressions,
  SUM(cp.clicks) AS clicks,
  SUM(cp.conversions) AS conversions,
  SUM(
    CASE
      WHEN cp.spend IS NULL THEN 0
      WHEN cp.spend::text LIKE '$%' THEN REPLACE(cp.spend::text,'$','')::numeric
      ELSE cp.spend::numeric
    END
  ) AS spend,
  SUM(cp.attributed_revenue) AS attributed_revenue,
  (SUM(cp.clicks)::numeric / NULLIF(SUM(cp.impressions),0))::numeric(10,4) AS ctr,
  (SUM(cp.conversions)::numeric / NULLIF(SUM(cp.clicks),0))::numeric(10,4) AS cvr,
  (SUM(cp.attributed_revenue)::numeric / NULLIF(
    SUM(
      CASE
        WHEN cp.spend IS NULL THEN 0
        WHEN cp.spend::text LIKE '$%' THEN REPLACE(cp.spend::text,'$','')::numeric
        ELSE cp.spend::numeric
      END
    ),0
  ))::numeric(10,4) AS roas
FROM silver.fact_campaign_performance cp
LEFT JOIN silver.dim_campaign c
  ON cp.campaign_id = c.campaign_id
GROUP BY 1,2,3,4;

--Funnel daily (already created in gold, just expose it)
CREATE OR REPLACE VIEW mart.funnel_daily AS
SELECT *
FROM gold.funnel_daily;

--Product quality signals (telemetry + warranty)
CREATE OR REPLACE VIEW mart.product_quality AS
SELECT
  dh.telemetry_date AS date,
  dh.product_type,
  dh.rows AS telemetry_rows,
  dh.avg_disconnects::numeric(10,3) AS avg_disconnects,
  dh.crash_rate::numeric(10,4) AS crash_rate,
  dh.avg_temp_c::numeric(10,2) AS avg_temp_c,
  wc.claims,
  wc.approved_claims,
  wc.claim_rate_per_1k_orders
FROM gold.device_health_daily dh
LEFT JOIN (
  SELECT
    p.product_type,
    COUNT(*) AS claims,
    SUM(CASE WHEN w.warranty_approved = 1 THEN 1 ELSE 0 END) AS approved_claims,
    (COUNT(*)::numeric / NULLIF(o.orders,0) * 1000)::numeric(10,2) AS claim_rate_per_1k_orders
  FROM silver.fact_warranty_claims w
  LEFT JOIN silver.dim_device d
    ON LOWER(TRIM(w.device_id)) = LOWER(TRIM(d.device_id))
  LEFT JOIN silver.dim_product p
    ON d.product_id = p.product_id
  LEFT JOIN (
    SELECT p2.product_type, COUNT(DISTINCT fo.order_id_clean) AS orders
    FROM silver.fact_orders fo
    LEFT JOIN silver.dim_product p2 ON fo.product_id_clean = p2.product_id
    GROUP BY 1
  ) o ON o.product_type = p.product_type
  GROUP BY 1, o.orders
) wc
ON wc.product_type = dh.product_type;

--Forecast accuracy (MAPE)
CREATE OR REPLACE VIEW mart.forecast_accuracy AS
SELECT
  month,
  region,
  product_type,
  AVG(
    CASE
      WHEN actual_units IS NULL THEN NULL
      WHEN actual_units::text ~ '^[0-9]+$' THEN
        ABS(actual_units::numeric - forecast_units::numeric) / NULLIF(actual_units::numeric,0)
      ELSE NULL
    END
  )::numeric(10,4) AS mape
FROM silver.fact_demand_forecast_vs_actual
GROUP BY 1,2,3;


--Data health (anti-joins)
CREATE OR REPLACE VIEW mart.data_health AS
SELECT * FROM (
  SELECT
    'web_customer_null_rate' AS metric,
    AVG(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)::numeric(10,4) AS value
  FROM silver.fact_web_events

  UNION ALL
  SELECT
    'purchase_missing_order_rate' AS metric,
    AVG(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)::numeric(10,4) AS value
  FROM silver.fact_web_events
  WHERE event_type = 'purchase'

  UNION ALL
  SELECT
    'shipments_without_orders_rate' AS metric,
    (COUNT(*) FILTER (WHERE o.order_id_clean IS NULL)::numeric / NULLIF(COUNT(*),0))::numeric(10,4) AS value
  FROM silver.fact_shipments s
  LEFT JOIN silver.fact_orders o
    ON s.order_id_clean = o.order_id_clean

  UNION ALL
  SELECT
    'warranty_without_orders_rate' AS metric,
    (COUNT(*) FILTER (WHERE o.order_id_clean IS NULL)::numeric / NULLIF(COUNT(*),0))::numeric(10,4) AS value
  FROM silver.fact_warranty_claims w
  LEFT JOIN silver.fact_orders o
    ON w.order_id_clean = o.order_id_clean
) t;

--
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'mart'
ORDER BY table_name;


SELECT * FROM mart.kpi_daily ORDER BY date DESC LIMIT 10;
SELECT * FROM mart.sales_mix ORDER BY revenue DESC LIMIT 10;
SELECT * FROM mart.shipping_sla ORDER BY late_rate DESC NULLS LAST LIMIT 10;
SELECT * FROM mart.marketing_performance ORDER BY roas DESC NULLS LAST LIMIT 10;
SELECT * FROM mart.data_health ORDER BY metric;








-- Q1: Top product types by revenue
SELECT product_type, SUM(revenue) AS revenue
FROM mart.sales_mix
GROUP BY 1
ORDER BY revenue DESC;

-- Q2: Which channels have the highest return rate?
SELECT channel_name, AVG(return_rate) AS avg_return_rate
FROM mart.sales_mix
GROUP BY 1
ORDER BY avg_return_rate DESC;

-- Q3: Which carriers are causing late deliveries?
SELECT carrier, AVG(late_rate) AS late_rate
FROM mart.shipping_sla
GROUP BY 1
ORDER BY late_rate DESC NULLS LAST;

-- Q4: Best campaigns by ROAS (min spend filter)
SELECT campaign_name, objective, region, spend, attributed_revenue, roas
FROM mart.marketing_performance
WHERE spend > 500
ORDER BY roas DESC NULLS LAST
LIMIT 15;

-- Q5: Funnel conversion hotspots
SELECT date, channel_name, product_type,
       COALESCE(product_view,0) AS product_view,
       COALESCE(add_to_cart,0) AS add_to_cart,
       COALESCE(purchase,0) AS purchase,
       pv_to_atc_rate, atc_to_purchase_rate
FROM mart.funnel_daily
ORDER BY date DESC
LIMIT 50;

-- Q6: Product quality issues (high crash rate / disconnects)
SELECT date, product_type, crash_rate, avg_disconnects, avg_temp_c, telemetry_rows
FROM mart.product_quality
ORDER BY crash_rate DESC NULLS LAST, avg_disconnects DESC NULLS LAST
LIMIT 20;

-- Q7: Forecast accuracy worst segments
SELECT region, product_type, AVG(mape) AS avg_mape
FROM mart.forecast_accuracy
GROUP BY 1,2
ORDER BY avg_mape DESC NULLS LAST;


SET enable_nestloop=0;

SELECT
    'postgresql' AS dbms,
    t.table_catalog,
    t.table_schema,
    t.table_name,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.character_maximum_length,
    n.constraint_type,
    k2.table_schema,
    k2.table_name,
    k2.column_name
FROM information_schema.tables t
NATURAL LEFT JOIN information_schema.columns c
LEFT JOIN (
    information_schema.key_column_usage k
    NATURAL JOIN information_schema.table_constraints n
    NATURAL LEFT JOIN information_schema.referential_constraints r
)
    ON c.table_catalog = k.table_catalog
   AND c.table_schema = k.table_schema
   AND c.table_name = k.table_name
   AND c.column_name = k.column_name
LEFT JOIN information_schema.key_column_usage k2
    ON k.position_in_unique_constraint = k2.ordinal_position
   AND r.unique_constraint_catalog = k2.constraint_catalog
   AND r.unique_constraint_schema = k2.constraint_schema
   AND r.unique_constraint_name = k2.constraint_name
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema = 'silver';
