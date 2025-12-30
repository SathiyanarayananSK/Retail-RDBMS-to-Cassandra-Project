from scripts.tools.queries import create_silver_sales_trends_by_country_month_table_cql, insert_silver_orders_by_country_month, silver_pipeline_metadata_cql, silver_pipeline_metadata_insert_cql
from cassandra.concurrent import execute_concurrent_with_args
import sys

class SalesTrendsByCountryDB:
    def __init__(self, retail_astra):
        # Connect to Astra
        self.retail_astra = retail_astra
        self.connection_astra = self.retail_astra.connection

        # Create Silver Table and process data
        self.check_infrastructure()
        self.process_bronze_to_silver_sales_trends_by_country_month()

    def check_infrastructure(self):
        try:
            # Check infrastructure for Silver Layer
            self.connection_astra.execute(create_silver_sales_trends_by_country_month_table_cql)
            self.connection_astra.execute(silver_pipeline_metadata_cql)
            self.connection_astra.execute(silver_pipeline_metadata_insert_cql)
            print("\nSilver layer infrastructure check complete.")
        except Exception as e:
            print(f"\nError during infra setup: {e}")
            sys.exit(1)

    def process_bronze_to_silver_sales_trends_by_country_month(self):
        print("\nStarting Silver Transformation...")

        # Get watermark from metadata table
        res = self.connection_astra.execute(
            "SELECT last_processed_id FROM pipeline_metadata WHERE pipeline_name = %s",
            ('bronze_to_silver_sales',)
        ).one()
        
        # Start from -1 (process everything) for no existing watermark or start from last processed id
        last_id = res.last_processed_id if res else -1
        print(f"Silver - Watermark found: Starting from Order ID {last_id}")

        # Get required rows from Bronze layer
        fetch_query = "SELECT * FROM bronze_orders WHERE order_id > %s ALLOW FILTERING"
        rows = list(self.connection_astra.execute(fetch_query, (last_id,)))

        if not rows:
            print("Silver layer is already up to date. No new data.")
            return

        print(f"Silver -Found {len(rows)} new rows. Transforming and Loading data...")

        # Prepare batch insert statement
        insert_stmt = self.connection_astra.prepare(insert_silver_orders_by_country_month)
        
        data_to_load = []
        max_id_in_batch = last_id

        for row in rows:
            # 1. Data cleaning (Skip rows with null critical fields)
            if row.customer_id is None or row.country is None or row.invoice_date is None:
                continue 
            
            # 2. Transformation Logic - create Shard Key (Hot Partition prevention)
            year_month_str = row.invoice_date.strftime('%Y-%m')

            # 3. Calculate Derived Column -> total_value
            qty = int(row.quantity) if row.quantity else 0
            price = float(row.unit_price) if row.unit_price else 0.0
            total_val = round(qty * price, 2)

            # Track the highest ID in this current batch to update the watermark later
            if row.order_id > max_id_in_batch:
                max_id_in_batch = row.order_id

            # Add to list for concurrent execution
            data_to_load.append((
                row.country, 
                year_month_str, 
                row.invoice_date, 
                row.order_id, 
                row.customer_id, 
                qty, 
                price, 
                total_val
            ))

        # Concurrent load - sends 100 requests in parallel instead of waiting for each one sequentially.
        if data_to_load:
            execute_concurrent_with_args(
                self.connection_astra, 
                insert_stmt, 
                data_to_load, 
                concurrency=100
            )

            # Update watermark after successful batch load
            self.connection_astra.execute(
                "UPDATE pipeline_metadata SET last_processed_id = %s WHERE pipeline_name = %s",
                (max_id_in_batch, 'bronze_to_silver_sales')
            )

            print(f"Silver Layer Loaded: {len(data_to_load)} rows processed. New Watermark: {max_id_in_batch}")

    