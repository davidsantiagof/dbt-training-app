CREATE WAREHOUSE newco_wh;
CREATE DATABASE newco_sources;
CREATE DATABASE newco_analytics_dev;
CREATE SCHEMA newco_sources.sales_sys;
CREATE SCHEMA newco_analytics_dev.snapshots;

--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.customer_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.order_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.event_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.stripe.payment_id_seq START = 1000 INCREMENT = 1;

--DROP DATABASE raw_sources;
--DROP DATABASE dev_analytics

CREATE ROLE dbt_dev_role;

GRANT USAGE ON WAREHOUSE newco_wh TO ROLE dbt_dev_role;
GRANT USAGE ON DATABASE newco_sources TO ROLE dbt_dev_role;

GRANT USAGE ON SCHEMA newco_sources.sales_sys TO ROLE dbt_dev_role;
GRANT SELECT ON ALL TABLES IN SCHEMA newco_sources.sales_sys TO ROLE dbt_dev_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA newco_sources.sales_sys TO ROLE dbt_dev_role;

GRANT USAGE ON DATABASE newco_analytics_dev TO ROLE dbt_dev_role;
GRANT CREATE SCHEMA ON DATABASE newco_analytics_dev TO ROLE dbt_dev_role;

GRANT USAGE ON SCHEMA newco_analytics_dev.snapshots TO ROLE dbt_dev_role;
GRANT CREATE TABLE ON SCHEMA newco_analytics_dev.snapshots TO ROLE dbt_dev_role;

SHOW GRANTS TO ROLE dbt_dev_role;

CREATE USER dflechas PASSWORD='1020747525';
GRANT ROLE dbt_dev_role TO USER dflechas;

SHOW GRANTS TO USER dflechas;

--GRANT USAGE ON SCHEMA dev_analytics.information_schema TO ROLE dbt_dev_role;
--GRANT SELECT ON ALL TABLES IN SCHEMA dev_analytics.information_schema TO ROLE dbt_dev_role;