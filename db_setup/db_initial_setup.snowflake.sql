CREATE WAREHOUSE transforming;
CREATE DATABASE raw_sources;
CREATE DATABASE dev_analytics;
CREATE SCHEMA raw_sources.jaffle_shop;
CREATE SCHEMA raw_sources.stripe;
CREATE SCHEMA dev_analytics.snapshots;

--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.customer_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.order_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.jaffle_shop.event_id_seq START = 1000 INCREMENT = 1;
--CREATE OR REPLACE SEQUENCE raw_sources.stripe.payment_id_seq START = 1000 INCREMENT = 1;

--DROP DATABASE raw_sources;
--DROP DATABASE dev_analytics

CREATE ROLE dbt_dev_role;

GRANT USAGE ON WAREHOUSE transforming TO ROLE dbt_dev_role;
GRANT USAGE ON DATABASE raw_sources TO ROLE dbt_dev_role;

GRANT USAGE ON SCHEMA raw_sources.jaffle_shop TO ROLE dbt_dev_role;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_sources.jaffle_shop TO ROLE dbt_dev_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA raw_sources.jaffle_shop TO ROLE dbt_dev_role;

GRANT USAGE ON SCHEMA raw_sources.stripe TO ROLE dbt_dev_role;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_sources.stripe TO ROLE dbt_dev_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA raw_sources.stripe TO ROLE dbt_dev_role;

GRANT USAGE ON DATABASE dev_analytics TO ROLE dbt_dev_role;
GRANT CREATE SCHEMA ON DATABASE dev_analytics TO ROLE dbt_dev_role;

--GRANT USAGE ON SCHEMA dev_analytics.information_schema TO ROLE dbt_dev_role;
--GRANT SELECT ON ALL TABLES IN SCHEMA dev_analytics.information_schema TO ROLE dbt_dev_role;

GRANT USAGE ON SCHEMA dev_analytics.snapshots TO ROLE dbt_dev_role;
GRANT CREATE TABLE ON SCHEMA dev_analytics.snapshots TO ROLE dbt_dev_role;

SHOW GRANTS TO ROLE dbt_dev_role;

CREATE USER dflechas PASSWORD='1020747525';
GRANT ROLE dbt_dev_role TO USER dflechas;

SHOW GRANTS TO USER dflechas;

