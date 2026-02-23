import os
import psycopg2
import socket
from dotenv import load_dotenv

load_dotenv()
class DbConnector():
    """ Class that handles database connection"""
    def __init__(self):
        self.database = os.getenv("DB_NAME")
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = os.getenv("DB_PORT") 
        self.connection = None

    def connect(self):
        """Gets database connection"""
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    dbname=self.database,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
            except psycopg2.Error as e:
                print(f"Error connecting to database: {e}")
                self.connection = None
                raise

        return self.connection
    def cursor(self):
        """Creates database cursor"""
        try:
            return self.connect().cursor()
        except psycopg2.Error as e:
            print(f"Error created cursor: {e}")
            self.connection = None

    def close(self):
        """Closes database connection"""
        if self.connection:
            try: 
                self.connection.close()
            except psycopg2.Error as e:
                print(f"Error closing connection: {e}")
            finally:
                self.connection = None


if __name__ == "__main__":
    db = DbConnector()
    try:
        conn = db.connect()
        print("Connection successful!")
    except socket.gaierror as e:
        print(f"Connection failed: {e}")
    finally:
        db.close()
