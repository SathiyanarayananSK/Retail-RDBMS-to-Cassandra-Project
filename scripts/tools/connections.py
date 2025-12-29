import sys, os
import psycopg
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider



# Class to connect to Neon(Postgres) and execute commands
class PostgresConnection():
    def __init__(self):
        load_dotenv()
        # Get the connection string from environment variable
        self.conn_string = os.getenv('neon_connection_string')
        # Connect to table
        self.connection = self.create_postgres_connection()
        self.cursor = self.connection.cursor()
        
    # Function to create Postgres connectio in Neon    
    def create_postgres_connection(self):
        try:
            if not self.conn_string:
                print("Connection string not found in .env file")
                sys.exit(1)
            # Connect using the connection string
            conn = psycopg.connect(self.conn_string)
            print("Successfully connected to Neon PostgreSQL using connection string")
            return conn
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)

    def execute_query(self, query):
        try:
            exec_op = self.cursor.execute(query)
            print(exec_op)
            self.connection.commit()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)

    def execute_copy(self, query, csv_file):
        try:
            
            with self.cursor.copy(query) as copy:
                copy.write(csv_file.read()) 
            self.cursor.execute("SELECT COUNT(*) FROM online_orders")
            row_count = self.cursor.fetchone()[0]
            print(f"Number of rows in table (from fetch): {row_count}")
            self.connection.commit()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)

    def close_connection(self):
        if self.connection:
            # Close the connection
            self.connection.close()


# Class to connect to Astra(Cassandra) and execute commands
class AstraDBConnection:
    def __init__(self):
        load_dotenv()
        self.bundle_path = 'secure-connect-retail-cassandra.zip'
        self.auth_provider = PlainTextAuthProvider('token', os.getenv('ASTRA_TOKEN'))

    def connect_to_the_cluster(self):
        # Connect to the Cluster
        cluster = Cluster(cloud={'secure_connect_bundle': self.bundle_path}, auth_provider=self.auth_provider)
        try:
            session = cluster.connect()
            session.set_keyspace("orders_keyspace")
            # Check the version
            row = session.execute("SELECT release_version FROM system.local").one()
            print("------------------------------------------")
            print(f"Success! Connected to Astra DB.")
            print(f"Cassandra Version: {row[0]}")
            print("------------------------------------------")
        except Exception as e:
            print(f"Connection Failed: {e}")
        finally:
            cluster.shutdown()
