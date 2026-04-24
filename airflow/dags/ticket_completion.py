import os
import sys
import json
import pendulum
from airflow.sdk import dag, task
from docker.types import Mount

from utils.asset_generation import generate_assets_for_tickets
from utils.db_connect import connect_to_db

DBT_ENV = {
    "DBT_PROFILES_DIR": "/usr/app",
    "DB_HOST": "postgres_container",
    "DB_PORT": os.environ['DB_PORT'],
    "DB_NAME": os.environ['DB_NAME'],
    "DB_USER": os.environ['DB_USER'],
    "DB_PASSWORD": os.environ['DB_PASSWORD'],
}

DBT_DOCKER_DEFAULTS = {
    "image": "content-intelligence-pipeline-dbt",
    "network_mode": "content-intelligence-pipeline_my-network",
    "mount_tmp_dir": False,
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "environment": DBT_ENV,
    "mounts": [
        Mount(
            target="/usr/app",
            source="/home/gabri/content-intelligence-pipeline/dbt",
            type="bind"
        )
    ]
}

@dag(
    schedule="1/1 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["content-intelligence-pipeline"],
    max_active_runs=1,
)
def complete_tickets():
    """
    Run full data pipeline.
    """
    @task
    def create_tickets_table():
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.tickets
            (
                id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                product_id integer,
                task text COLLATE pg_catalog."default",
                priority text COLLATE pg_catalog."default",
                status text COLLATE pg_catalog."default",
                completed boolean DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()

    @task
    def generate_assets():
        """
        Generate assets for the tickets.
        """
        generate_assets_for_tickets()
    
    create_tickets_table() >> generate_assets()
    
complete_tickets()