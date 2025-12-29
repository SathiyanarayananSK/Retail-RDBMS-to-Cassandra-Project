from apscheduler.schedulers.background import BackgroundScheduler
from scripts.data_to_bronze import SyncBronze
from scripts.tools.connections import PostgresConnection, AstraDBConnection
from scripts.rdbms_data_loader import RDBMSDataLoader
import time


if __name__ == "__main__":
    # Connect to postgres
    retail_postgres = PostgresConnection()
    rdbms_data_loader = RDBMSDataLoader(retail_postgres)
    
    # Connect to Astra
    retail_astra = AstraDBConnection()
    bronze_layer_sync = SyncBronze(retail_postgres, retail_astra)

    scheduler = BackgroundScheduler()

    # Job schedule to load data from csv to postgres in batches
    scheduler.add_job(rdbms_data_loader.run_batch_loader, trigger="interval", seconds=30)

    # Job schedule to sync data from Postgres to Astra Bronze table
    scheduler.add_job(bronze_layer_sync.run_bronze_sync, trigger="interval", seconds=13)

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        retail_postgres.close_connection()
