import sys
import json
import pendulum
from airflow.sdk import dag, task

from utils.api_request import get_products
from utils.insert_records import ingest_data
from utils.llm_enrich import enrich_data

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def run_pipeline():
    """
    Run full data pipeline.
    """
    @task()
    def ingest():
        """
        Retrieve product data from API and load it into PostgreSQL.
        """
        ingest_data()
        
    @task()
    def enrich():
        """
        Use LLM to enrich the product data with marketing copy.
        """
        enrich_data()
    
    ingest()
    enrich()
    
run_pipeline()
