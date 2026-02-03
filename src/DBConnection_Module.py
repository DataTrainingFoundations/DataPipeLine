import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()
class DbConnector():


    def __init__(self):
        self.driver = os.getenv("MONGO_ODBC_DRIVER")
        self.database = os.getenv("MONGO_DB")
        self.host = os.getenv("MONGO_HOST")
        self.user = os.getenv("MONGO_USER")
        self.password = os.getenv("MONGO_PASSWORD")
        self.port = os.getenv("MONGO_PORT") 

        self.connection = None

    def connect(self):
        if self.connection is None:
            try:
                conn_str = (
                    f"DRIVER={{{self.driver}}};"
                    f"SERVER={self.host},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.user};"
                    f"PWD={self.password};"
                )
                self.connection = pyodbc.connect(conn_str)
            except pyodbc.Error as e:
                print(f"Error connecting to databse: {e}")
                self.connection = None
                raise

        return self.connection
    def cursor(self):
        try:
            return self.connect().cursor()
        except pyodbc.Error as e:
            print(f"Error created cursor: {e}")
        finally:
            self.connection = None

    def close(self):
        if self.connection:
            try: 
                self.connection.close()
            except pyodbc.Error as e:
                print(f"Error closing connection: {e}")
            finally:
                self.connection = None



if __name__ == "__main__":
    db = DbConnector()
    try:
        conn = db.connect()
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {e}")
    finally:
        db.close()

