import sys, os
import psycopg
from dotenv import load_dotenv



class PostgresConnection():
    # Initiator method to connect to postgres
    def __init__(self):
        load_dotenv()
        self.connection = self.create_postgres_connection()

    
    # Function to create Postgres connectio in Neon    
    def create_postgres_connection(self):
        """Create connection to Neon PostgreSQL using connection string"""
        try:
            # Get the connection string from environment variable
            conn_string = os.getenv('neon_connection_string')
            if not conn_string:
                print("Connection string not found in .env file")
                sys.exit(1)
            
            # Connect using the connection string
            conn = psycopg.connect(conn_string)
            print("Successfully connected to Neon PostgreSQL using connection string")
            return conn
        except psycopg.OperationalError as e:
            print(f"Connection failed. Check your connection string: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)

    def execute_command(self, command=""):
        with self.connection.cursor() as cur:
            # Customers
            exec_op = cur.execute(command)
            commit_op = self.connection.commit()
            print(exec_op, "\n", commit_op)

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed")
