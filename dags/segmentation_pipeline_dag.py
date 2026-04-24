"""
Apache Airflow DAG — Customer Segmentation Pipeline
=====================================================
Schedules the segmentation pipeline to run every
Sunday at 04:00 AM UTC (weekly re-segmentation).

Tasks:
    1. extract_customers    — generate/load customer data
    2. run_segmentation     — RFM features + K-Means clustering
    3. load_to_postgres     — load segments into PostgreSQL
    4. refresh_dbt          — rebuild analytical layer
    5. notify_marketing     — log segment summary

Author: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="customer_segmentation_pipeline",
    default_args=default_args,
    description="Weekly customer RFM segmentation pipeline",
    schedule_interval="0 4 * * 0",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["segmentation","rfm","kmeans","ghana","data-engineering"],
)


def task_extract(**context):
    from customer_segmentation import extract
    df = extract()
    temp_path = "/tmp/customers_raw.csv"
    df.to_csv(temp_path, index=False)
    context["ti"].xcom_push(key="raw_count", value=len(df))
    context["ti"].xcom_push(key="temp_path", value=temp_path)
    logger.info(f"Extracted {len(df):,} customer records")
    return len(df)


def task_segment(**context):
    import pandas as pd
    from customer_segmentation import transform
    temp_path = context["ti"].xcom_pull(task_ids="extract_customers", key="temp_path")
    df = pd.read_csv(temp_path)
    df["last_purchase_date"] = pd.to_datetime(df["last_purchase_date"]).dt.date
    result = transform(df)
    clean_path = "/tmp/customers_segmented.csv"
    result.to_csv(clean_path, index=False)
    champions = int((result["segment_name"] == "Champions").sum())
    at_risk   = int((result["segment_name"] == "At Risk").sum())
    context["ti"].xcom_push(key="clean_path",  value=clean_path)
    context["ti"].xcom_push(key="champions",   value=champions)
    context["ti"].xcom_push(key="at_risk",     value=at_risk)
    logger.info(f"Segmented {len(result):,} customers")
    return len(result)


def task_load(**context):
    import pandas as pd
    from customer_segmentation import transform, load
    temp_path = context["ti"].xcom_pull(task_ids="extract_customers", key="temp_path")
    df = pd.read_csv(temp_path)
    df["last_purchase_date"] = pd.to_datetime(df["last_purchase_date"]).dt.date
    result = transform(df)
    load(result)
    logger.info("Load to PostgreSQL complete")
    return "success"


def task_notify(**context):
    run_date   = context["ds"]
    raw        = context["ti"].xcom_pull(task_ids="extract_customers",  key="raw_count")
    champions  = context["ti"].xcom_pull(task_ids="run_segmentation",   key="champions")
    at_risk    = context["ti"].xcom_pull(task_ids="run_segmentation",   key="at_risk")
    logger.info("=" * 60)
    logger.info("  SEGMENTATION PIPELINE — WEEKLY MARKETING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Run Date          : {run_date}")
    logger.info(f"  Customers Scored  : {raw:,}")
    logger.info(f"  Champions         : {champions:,}")
    logger.info(f"  At Risk           : {at_risk:,}")
    logger.info("=" * 60)
    return "notified"


start          = EmptyOperator(task_id="pipeline_start",    dag=dag)
extract_task   = PythonOperator(task_id="extract_customers",  python_callable=task_extract,  dag=dag)
segment_task   = PythonOperator(task_id="run_segmentation",   python_callable=task_segment,  dag=dag)
load_task      = PythonOperator(task_id="load_to_postgres",   python_callable=task_load,     dag=dag)
notify_task    = PythonOperator(task_id="notify_marketing",   python_callable=task_notify,   dag=dag)
end            = EmptyOperator(task_id="pipeline_end",      dag=dag)

start >> extract_task >> segment_task >> load_task >> notify_task >> end