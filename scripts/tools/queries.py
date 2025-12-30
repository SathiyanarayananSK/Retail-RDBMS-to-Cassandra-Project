create_online_orders_table_sql = """
CREATE TABLE IF NOT EXISTS online_orders (
order_id SERIAL,
invoice_no VARCHAR(50),
stock_code VARCHAR(50),
description TEXT,
quantity INTEGER,
invoice_date TIMESTAMP,
unit_price DECIMAL(10,2),
customer_id INTEGER,
country VARCHAR(75),
ingested_at TIMESTAMP,
PRIMARY KEY(order_id)
);"""

copy_online_orders_sql = """
COPY online_orders(
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country,
    ingested_at
)
FROM STDIN
WITH CSV HEADER
"""

create_bronze_orders_table_cql = """
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id int,
    invoice_no text,
    stock_code text,
    description text,
    quantity int,
    invoice_date timestamp,
    unit_price float,
    customer_id int,
    country text,
    ingested_at timestamp,
    cassandra_injested_at timestamp,
    PRIMARY KEY((customer_id),order_id)
) WITH CLUSTERING ORDER BY (order_id ASC);
"""

insert_orders_into_bronze_cql = """
INSERT INTO bronze_orders (
    order_id, invoice_no, stock_code, description, 
    quantity, invoice_date, unit_price, customer_id, 
    country, ingested_at, cassandra_injested_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

create_silver_sales_trends_by_country_month_table_cql = """
CREATE TABLE IF NOT EXISTS silver_orders_by_country_month (
    country text,
    year_month text,
    invoice_date timestamp,
    order_id int,
    customer_id int,
    quantity int,
    unit_price float,
    total_value float,
    PRIMARY KEY ((country, year_month), invoice_date, order_id)
) WITH CLUSTERING ORDER BY (invoice_date DESC, order_id ASC);
"""

insert_silver_orders_by_country_month = """
INSERT INTO silver_orders_by_country_month (
    country, year_month, invoice_date, order_id, 
    customer_id, quantity, unit_price, total_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

silver_pipeline_metadata_cql = """
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    pipeline_name text PRIMARY KEY, last_processed_id int)
"""
silver_pipeline_metadata_insert_cql = """
INSERT INTO pipeline_metadata (pipeline_name, last_processed_id) 
VALUES ('bronze_to_silver_sales', -1) 
IF NOT EXISTS;
"""