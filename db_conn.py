import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
import psycopg2.extras

load_dotenv(find_dotenv())

class DatabaseConnection():
    connect_ = None
    cursor_ = None
    
    @classmethod
    def __get_db_connection(cls):
        env = os.getenv("ENV", "DEVELOPMENT")
        # print(env)
        if env=="STAGING":
            conn = psycopg2.connect(host=os.getenv('POSTGRES_HOST'),
                                    database=os.getenv('POSTGRES_NAME'),
                                    user=os.getenv('POSTGRES_USER'),
                                    password=os.getenv('POSTGRES_PASSWORD'),
                                    port=5432)
            
        elif env == "DEVELOPMENT":
            conn = psycopg2.connect(host="localhost",
                                    database=os.getenv('DB_LOCAL'),
                                    user="postgres",
                                    password=os.getenv('DB_LOCAL_PASS'),
                                    port=5432)
        
        return conn
    
    def get_cursor(self):
        DatabaseConnection.connect_ = DatabaseConnection.__get_db_connection()
        
        DatabaseConnection.cursor_ = DatabaseConnection.connect_.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return DatabaseConnection.cursor_
    
    def close_connection(self):
        DatabaseConnection.cursor_.close()
        DatabaseConnection.connect_.close()
        return 