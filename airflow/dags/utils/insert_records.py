import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, UTC
from pprint import pprint
import json
import os

from utils.api_request import get_products
from utils.db_connect import connect_to_db
    
def create_raw_table(conn):
    print('Ensuring table exists...')
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.products (
                id INTEGER PRIMARY KEY,
                title TEXT,
                description TEXT,
                price NUMERIC,
                category TEXT,
                image TEXT,
                rating JSONB,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
        print('raw.products table created')
    except psycopg2.Error as e:
        print(f'Failed to create raw.products table: {e}')
        raise
    
def insert_records(conn, data):
    """
    Load product data into transient staging table (truncate first).
    """
    print('Inserting data into raw.products...')
    try:
        # Set ingested_at timestamp and convert rating JSON to string
        ingested_at = datetime.now(UTC)
        for d in data:
            d['rating'] = json.dumps(d.get('rating', {}))
            d['ingested_at'] = ingested_at
            
        # Deduplicate and warn if duplicates are present
        seen = {}
        for d in data:
            if d['id'] in seen:
                print(f"Warning: duplicate product ID {d['id']} in source data")
            else:
                seen[d['id']] = d
        data = list(seen.values())
                
        # Connect and execute TRUNCATE and INSERT statement
        values = [
            (d['id'], d['title'], d['description'], d['price'],
            d['category'], d['image'], d['rating'], d['ingested_at'])
            for d in data[:5]
        ]
        
        cursor = conn.cursor()
        execute_values(
            cursor,
            """
            INSERT INTO raw.products (
                id,
                title,
                description,
                price,
                category,
                image,
                rating,
                ingested_at
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                price = EXCLUDED.price,
                category = EXCLUDED.category,
                image = EXCLUDED.image,
                rating = EXCLUDED.rating,
                ingested_at = EXCLUDED.ingested_at           
            """,
            values
        )
        conn.commit()
        print('Data inserted successfully into raw.products')
    except psycopg2.Error as e:
        print(f'Failed to insert records into raw.products: {e}')
        raise
    
    
def ingest_data():
    try:
        data = get_products()
        conn = connect_to_db()
        create_raw_table(conn)
        insert_records(conn, data) 
    except Exception as e:
        print(f'An error occurred during pipeline run: {e}')
    finally:
        if 'conn' in locals():
            conn.close()
            print('DB connection closed')
