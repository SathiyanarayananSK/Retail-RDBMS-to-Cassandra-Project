import pandas as pd
from connections import PostgresConnection



test_customers = PostgresConnection()

# test_customers.execute_command("""CREATE TABLE IF NOT EXISTS customers (
#                                                     customer_id SERIAL PRIMARY KEY,
#                                                     name TEXT NOT NULL,
#                                                     email TEXT UNIQUE NOT NULL,
#                                                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#                                                     );""")

test_customers.execute_command("""
                               INSERT INTO customers (name, email) VALUES
                               ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')
                               ON CONFLICT DO NOTHING;
                               """)

test_customers.close_connection()