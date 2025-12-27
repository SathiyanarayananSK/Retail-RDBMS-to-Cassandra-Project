import pandas as pd
from io import StringIO
from connections import PostgresConnection
import sql_queries
import time
from datetime import datetime
from random import randint

def prepare_csv_data():
    # Read and prepare csv data
    orders_df_local = pd.read_csv("../../data/online_retail.csv")
    orders_df_local.drop(columns=["index"], inplace=True)
    orders_df_local.rename(columns=
                     {"OrderId":"order_id",
                      "InvoiceNo":"invoice_no",
                      "StockCode":"stock_code",
                      "Description":"description",
                      "Quantity":"quantity",
                      "InvoiceDate":"invoice_date",
                      "UnitPrice":"unit_price",
                      "CustomerID":"customer_id",
                      "Country":"country"}, inplace=True)
    orders_df_local['customer_id'] = orders_df_local['customer_id'].fillna(0).astype(int)
    orders_df_local["invoice_date"] = pd.to_datetime(
    orders_df_local["invoice_date"],
    format="%d/%m/%Y %H:%M",
    errors="coerce"
    )
    return orders_df_local


if __name__ =="__main__":
    # Create table object and a table in neon-postgres db
    online_orders = PostgresConnection()
    online_orders.execute_query(sql_queries.create_online_orders_table)

    # Read and prepare the csv from disc
    orders_df = prepare_csv_data()
    total_rows = len(orders_df)
    current_index = 0

    while current_index < total_rows:
        # Random batch size between 1000 and 10000
        batch_size = randint(1000, 10000)
        end_index = min(current_index + batch_size, total_rows)

        batch_df = orders_df.iloc[current_index:end_index].copy()
        # Add current timestamp column
        batch_df['ingested_at'] = datetime.now()
        

        print(f"Inserting rows {current_index} to {end_index} (batch size: {len(batch_df)})")

        # Prepare in-memory CSV for COPY
        buffer = StringIO()
        batch_df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        # Copy the batch to the db
        online_orders.execute_copy(sql_queries.copy_online_orders, buffer)

        # Update index
        current_index = end_index

        # Delay before next batch
        delay_seconds = 10
        print(f"Next batch coming in {delay_seconds} seconds!\n")
        time.sleep(delay_seconds)

    # Close the connection
    online_orders.close_connection()

