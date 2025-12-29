from scripts.tools.connections import AstraDBConnection
from cassandra.concurrent import execute_concurrent_with_args
from datetime import datetime
from scripts.tools.queries import create_bronze_orders_table_cql, insert_orders_into_bronze_cql
import sys



class SyncBronze:
    def __init__(self, retail_pg, retail_astra):
        # Connect to Postgres
        self.retail_pg = retail_pg
        self.connection_pg = self.retail_pg.cursor
        
        # Connect to Astra
        self.retail_astra = retail_astra
        self.connection_astra = self.retail_astra.connection

        # Create table and sync data
        self.create_bronze_table()
        self.run_bronze_sync()

    # create the bronze table in Astra
    def create_bronze_table(self):
        try:
            self.connection_astra.execute(create_bronze_orders_table_cql)
            print("Bronze table created or already exists.")
        except Exception as e:
            print(f"Error creating bronze table: {e}")
            sys.exit(1)

    # Sync data from Postgres to Astra Bronze table
    def run_bronze_sync(self):
        print("\nSyncing data from Postgres to Astra Bronze...")
        # get the highest ID from astra to never duplicate work
        result = self.connection_astra.execute("SELECT MAX(order_id) FROM bronze_orders").one()
        last_synced_id = result[0] if result[0] is not None else -1
        print(f"Watermark found: {last_synced_id}")

        # fetch everything new from Postgres
        pg_query = "SELECT * FROM online_orders WHERE order_id > %s ORDER BY order_id ASC"
        self.connection_pg.execute(pg_query, (last_synced_id,))
        rows = self.connection_pg.fetchall()
        
        if not rows:
            print("Data is already in sync. Nothing to do.")
            return

        print(f"Found {len(rows)} new rows in Postgres. Preparing transfer...")

        # prepare and load the data into Astra Bronze
        
        prepared = self.connection_astra.prepare(insert_orders_into_bronze_cql)

        # map the Postgres data to the Cassandra schema with ingested timestamp
        data_to_insert = [
            (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], datetime.now()) 
            for r in rows
        ]

        # execute_concurrent_with_args
        execute_concurrent_with_args(self.connection_astra, prepared, data_to_insert, concurrency=100)
        
        print(f"Successfully synced {len(rows)} rows to Astra Bronze.")