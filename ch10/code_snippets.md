# Chapter Code Snippets

In this file you'll find the code snippets in this chapter

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json

def load_config():
    with open('/configs/table_policies.json') as f:
        return json.load(f)

def run_compaction(table_name, file_size_threshold, snapshot_retention):
    print(f"Running compaction for {table_name}")
    print(f"File size threshold: {file_size_threshold}MB") 
    print(f"Retaining {snapshot_retention} snapshots")   
    # Placeholder for actual compaction logic
    # For example: spark.sql(f"CALL ...")

default_args = {'start_date': datetime(2023, 1, 1)}

with DAG('table_maintenance',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    config = load_config()                                  

    for table, policy in config.items():
        task = PythonOperator(
            task_id=f"compact_{table}",
            python_callable=run_compaction,
            op_kwargs={
                'table_name': table,
                'file_size_threshold': policy['file_size_threshold'],
                'snapshot_retention': policy['snapshot_retention']
            }                                                 
        )
```


```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'iceberg_compaction_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
)

def should_compact():
    result = subprocess.check_output([
        "spark-sql",
        "-e",
        "SELECT count(*) FROM mydb.mytable.files WHERE file_size_in_bytes < 20000000;"
    ])
    return int(result.strip()) > 500 

check_compaction = PythonOperator(
    task_id='check_file_fragmentation',
    python_callable=should_compact,
    dag=dag,
)

compact_table = BashOperator(
    task_id='run_compaction',
    bash_command='spark-sql -e "CALL mydb.system.rewrite_data_files(table => \'mydb.mytable\')"', 
    dag=dag,
)

check_compaction >> compact_table
```

```sql
-- models/transform_customer_data.sql
SELECT *
FROM raw.customers
WHERE updated_at > CURRENT_DATE - INTERVAL '30 days'


-- macros/expire_snapshots.sql
{% macro expire_snapshots(table_name) %}
    CALL {{ table_name }}.system.expire_snapshots(
        older_than => CURRENT_TIMESTAMP - INTERVAL '7 days'
    );
{% endmacro %}
```

```shell
dbt run --select transform_customer_data
dbt run-operation expire_snapshots --args '{"table_name": "warehouse.customers"}'
```

```python
import requests
import subprocess

def get_snapshot_count():
    result = subprocess.check_output([
        "spark-sql",
        "-e",
        "SELECT count(*) FROM mydb.mytable.snapshots"
    ])
    return int(result.strip())

snapshot_count = get_snapshot_count()

requests.post(
    "http://localhost:9091/metrics/job/iceberg_table_monitoring",
    data=f"iceberg_snapshot_count{{table=\"mydb.mytable\"}} {snapshot_count}\n"
)
```

```sql
SELECT f.file_path
FROM my_table.files f
LEFT JOIN my_table.metadata_log_entries m
  ON f.file_path = m.file
WHERE m.operation = 'DELETE'
  AND f.partition = 'region=EU'
  AND f.last_modified < DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY);
```

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM "db"."critical_fact_table.snapshots"
ORDER BY committed_at DESC;
```

```sql
SELECT ref_name, type, snapshot_id, max_reference_age_in_ms
FROM "db"."critical_fact_table.refs";
```

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'dataops',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def expire_snapshots():
    subprocess.run([
        "spark-submit",
        "--class", "org.apache.iceberg.actions.ExpireSnapshots",
        "--master", "local",
        "maintenance.jar",
        "--table", "db.sales",
        "--older-than", "7d"
    ])                                                  

def remove_orphans():
    subprocess.run([
        "spark-submit",
        "--class", "org.apache.iceberg.actions.RemoveOrphanFiles",
        "--master", "local",
        "maintenance.jar",
        "--table", "db.sales",
        "--older-than", "3d"
    ])                                                 

with DAG('iceberg_retention_policy',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    expire = PythonOperator(
        task_id='expire_old_snapshots',
        python_callable=expire_snapshots
    )

    cleanup = PythonOperator(
        task_id='remove_orphan_files',
        python_callable=remove_orphans
    )

    expire >> cleanup  
```

```python
from openlineage.spark import OpenLineageSparkListener
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("iceberg-lineage-tracking") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://my-bucket/warehouse") \
    .config("spark.extraListeners", "openlineage.spark.agent.OpenLineageSparkListener") \ 
    .config("openlineage.transport.type", "http") \
    .config("openlineage.transport.url", "http://openlineage-collector:5000") \            
    .getOrCreate()

df = spark.read.table("my_catalog.db.critical_fact_table")
df.filter("region = 'US'").writeTo("my_catalog.db.reporting_fact_table").overwrite() 
```

```sql
SELECT 
  snapshot_id, 
  operation,       
  committed_at,    
  user_id          
FROM my_catalog.db.table.snapshots
ORDER BY committed_at DESC;
```

```python
from openlineage.client.run import RunEvent, RunState
from openlineage.client.facet import SourceCodeLocationRunFacet

RunEvent(
    eventType=RunState.START,
    job={
        "namespace": "datalake-engine",
        "name": "daily_etl_job"
    },
    run={
        "runId": "abcd-1234"
    },
    producer="https://my.lineage.collector",
    facets={
        "sourceCodeLocation": SourceCodeLocationRunFacet(
            type="git",
            url="https://github.com/my-org/etl-jobs",
            branch="main"
        )
    }
)
```

```sql
SELECT 
  COUNT(*) AS commits,
  MAX(committed_at) AS last_commit,
  MIN(committed_at) AS first_commit
FROM my_catalog.db.sales.snapshots
WHERE committed_at > current_date - INTERVAL '30' DAY
```

```sql
CALL my_catalog.system.register_table(
  table => 'recovered_db.recovered_table',
  metadata_file => 's3://my-bucket/iceberg/metadata/metadata-000123.json'
);

CALL my_catalog.system.set_current_snapshot(
  table => 'recovered_db.recovered_table',
  snapshot_id => 123
);
```

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='iceberg_recovery_dag',
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    rollback = SparkSqlOperator(
        task_id='rollback_snapshot',
        sql="""
            CALL my_catalog.system.rollback_to_snapshot(
              table => 'prod.sales',
              snapshot_id => 205
            )
        """, 
        spark_conn_id='spark_default'
    )

    expire = SparkSqlOperator(
        task_id='expire_newer_snapshots',
        sql="""
            CALL my_catalog.system.expire_snapshots(
              table => 'prod.sales',
              older_than => TIMESTAMP '2025-09-10 00:00:00'
            )
        """,
        spark_conn_id='spark_default'
    )

    rollback >> expire
```

```sql
SELECT file_path
FROM my_catalog.prod.sales.manifests
WHERE NOT EXISTS (
  SELECT 1
  FROM my_catalog.prod.sales.files
  WHERE files.file_path = manifests.file_path
);
```

```sql
CALL nessie.catalog.branch(
  name => 'recovery_zone',
  from_ref => 'main'
);

CALL nessie.catalog.fast_forward(
  table => 'prod.sales',
  branch => 'recovery_zone',
  to => 'main'
);
```