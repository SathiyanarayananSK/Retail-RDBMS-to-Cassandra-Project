import pandas as pd
from io import StringIO
import scripts.tools.queries as queries
from datetime import datetime
from random import randint


class RDBMSDataLoader:
    def __init__(self, online_orders):
        # Create table object and a table in neon-postgres db
        self.online_orders = online_orders
        self.online_orders.execute_query(queries.create_online_orders_table_sql)

        # Read and prepare the csv from disc
        self.orders_df = self.prepare_csv_data()
        self.total_rows = len(self.orders_df)
        self.current_index = 0
        self.run_batch_loader()

    def run_batch_loader(self):
        if self.current_index < self.total_rows:
            print("")
            # Random batch size between 1000 and 5000
            batch_size = randint(1000, 5000)
            end_index = min(self.current_index + batch_size, self.total_rows)

            batch_df = self.orders_df.iloc[self.current_index:end_index].copy()
            
            # Add current timestamp column
            batch_df['ingested_at'] = datetime.now()
            
            print(f"\nLoading data from csv to Postgres in batches...Inserting rows {self.current_index} to {end_index} (batch size: {len(batch_df)})")

            # Prepare in-memory CSV for COPY
            buffer = StringIO()
            batch_df.to_csv(buffer, index=False, header=True)
            buffer.seek(0)

            # Copy the batch to the db
            self.online_orders.execute_copy(queries.copy_online_orders_sql, buffer)

            # Update index
            self.current_index = end_index

        else:
            print("No new orders!")

    @staticmethod
    def prepare_csv_data():
        # Read and prepare csv data
        orders_df_local = pd.read_csv("data/online_retail.csv")
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
