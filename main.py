from apscheduler.schedulers.background import BackgroundScheduler
from scripts.data_to_bronze import SyncBronze
from scripts.tools.connections import PostgresConnection, AstraDBConnection
from scripts.rdbms_data_loader import RDBMSDataLoader
from scripts.silver_sales_trends_by_country_migration import SalesTrendsByCountryDB
import time


if __name__ == "__main__":
    # Connect to postgres
    retail_postgres = PostgresConnection()
    rdbms_data_loader = RDBMSDataLoader(retail_postgres)
    
    # Connect to Astra
    retail_astra = AstraDBConnection()

    # Initialize sync and transformation classes
    bronze_layer_sync = SyncBronze(retail_postgres, retail_astra)
    Silver_sales_trends_by_country_month = SalesTrendsByCountryDB(retail_astra)
    
    # Initialize the scheduler
    scheduler = BackgroundScheduler()

    # Job schedule to load data from csv to postgres in batches
    scheduler.add_job(rdbms_data_loader.run_batch_loader, trigger="interval", seconds=45)

    # Job schedule to sync data from Postgres to Astra Bronze table
    scheduler.add_job(bronze_layer_sync.run_bronze_sync, trigger="interval", seconds=12)

    # Job schedule to tansform and load data from Bronze to Silver table
    scheduler.add_job(Silver_sales_trends_by_country_month.process_bronze_to_silver_sales_trends_by_country_month, trigger="interval", seconds=20)


    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        retail_postgres.close_connection()
        retail_astra.close_connection()


    
