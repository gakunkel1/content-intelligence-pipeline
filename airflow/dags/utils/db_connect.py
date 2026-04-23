import os
import psycopg2

def connect_to_db():
    print('Connecting to PostgreSQL')
    try:
        return psycopg2.connect(
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            dbname=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
        )
    except psycopg2.Error as e:
        print(f'Database connection failed: {e}')
        raise