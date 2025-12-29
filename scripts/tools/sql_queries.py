create_online_orders_table = """
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

copy_online_orders = """
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