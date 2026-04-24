from airflow.providers.docker.operators.docker import DockerOperator
import os
import sys
import json
import pendulum
from airflow.sdk import dag, task
from docker.types import Mount

from utils.insert_records import ingest_data
from utils.llm_enrich import enrich_data

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
    "force_pull": False,
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
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["content-intelligence-pipeline"],
    max_active_runs=1,
)
def run_pipeline():
    """
    Run full data pipeline.
    """
    @task
    def ingest_products():
        """
        Retrieve product data from API and load it into PostgreSQL.
        """
        ingest_data()

    # Create products staging model
    dbt_stg_products = DockerOperator(
        task_id="dbt_stg_products",
        command="run --select stg_products --project-dir /usr/app/content_intelligence_pipeline",
        **DBT_DOCKER_DEFAULTS,
    )

    # Create snapshot of product data
    dbt_product_snapshot = DockerOperator(
        task_id="dbt_product_snapshot",
        command="snapshot --select product_snapshot --project-dir /usr/app/content_intelligence_pipeline",
        **DBT_DOCKER_DEFAULTS,
    )
        
    @task
    def enrich_products():
        """
        Use LLM to enrich the product data with marketing copy.
        """
        enrich_data()

    dbt_stg_analytics_models = DockerOperator(
        task_id="dbt_stg_analytics_models",
        command="run --select tag:stg_analytics --project-dir /usr/app/content_intelligence_pipeline",
        **DBT_DOCKER_DEFAULTS,
    )
        
    dbt_analytics_models = DockerOperator(
        task_id="dbt_analytics_models",
        command="run --select tag:analytics --project-dir /usr/app/content_intelligence_pipeline",
        **DBT_DOCKER_DEFAULTS,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        command="test --project-dir /usr/app/content_intelligence_pipeline",
        **DBT_DOCKER_DEFAULTS,
    )
    
    ingest_products() >> dbt_stg_products >> dbt_product_snapshot >> enrich_products() >> dbt_stg_analytics_models >> dbt_analytics_models >> dbt_test
    
run_pipeline()