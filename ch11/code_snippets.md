# Chapter 11 Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
from airflow import DAG  
from airflow.operators.python_operator import PythonOperator  
from datetime import datetime  
import json

def load_config():  
    with open('/configs/table_policies.json') as f:  
        return json.load(f)

def run_compaction(table_name, file_size_threshold, snapshot_retention):  
    print(f"Running compaction for {table_name}")  
    print(f"File size threshold: {file_size_threshold}MB")     #A  
    print(f"Retaining {snapshot_retention} snapshots")         #B  
    # Placeholder for actual compaction logic  
    # For example: spark.sql(f"CALL ...")

default_args = {'start_date': datetime(2023, 1, 1)}

with DAG('table_maintenance',  
         default_args=default_args,  
         schedule_interval='@daily',  
         catchup=False) as dag:

    config = load_config()                                      #C

    for table, policy in config.items():  
        task = PythonOperator(  
            task_id=f"compact_{table}",  
            python_callable=run_compaction,  
            op_kwargs={  
                'table_name': table,  
                'file_size_threshold': policy['file_size_threshold'],  
                'snapshot_retention': policy['snapshot_retention']  
            }                                                   #D  
        )
```

## Snippet 2

```text
[2025-10-01 02:00:12,345] {taskinstance.py:1064} INFO - Starting task: compact_orders_table  
[2025-10-01 02:00:13,678] {sql.py:76} INFO - Running SQL: CALL iceberg.system.rewrite_data_files('db.orders')  
[2025-10-01 02:00:20,982] {sql.py:81} ERROR - Compaction failed: Too many open files  
[2025-10-01 02:00:21,104] {taskinstance.py:1234} ERROR - Task failed with exception: OSError: [Errno 24] Too many open files  
[2025-10-01 02:00:21,105] {taskinstance.py:1300} INFO - Marking task as FAILED  
[2025-10-01 02:00:21,200] {taskinstance.py:1421} INFO - Triggering alert: maintenance.compaction_failure
```

## Snippet 3

```text
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
        "SELECT count(*) FROM mydb.mytable.files WHERE file_size_in_bytes < 20000000;"  #A  
    ])  
    return int(result.strip()) > 500  #B

check_compaction = PythonOperator(  
    task_id='check_file_fragmentation',  
    python_callable=should_compact,  
    dag=dag,  
)

compact_table = BashOperator(  
    task_id='run_compaction',  
    bash_command='spark-sql -e "CALL mydb.system.rewrite_data_files(table => \'mydb.mytable\')"',  #C  
    dag=dag,  
)

check_compaction >> compact_table  #D
```

## Snippet 4

```text
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

## Snippet 5

```text
dbt run --select transform_customer_data  
dbt run-operation expire_snapshots --args '{"table_name": "warehouse.customers"}'  #A
```

## Snippet 6

```text
import requests  
import subprocess

def get_snapshot_count():  
    result = subprocess.check_output([  
        "spark-sql",  
        "-e",  
        "SELECT count(*) FROM mydb.mytable.snapshots"  
    ])  
    return int(result.strip())

snapshot_count = get_snapshot_count()  #A

requests.post(  
    "http://localhost:9091/metrics/job/iceberg_table_monitoring",  
    data=f"iceberg_snapshot_count{{table=\"mydb.mytable\"}} {snapshot_count}\n"  #B  
)
```

## Snippet 7

```text
SELECT f.file_path  
FROM my_table.files f  
LEFT JOIN my_table.metadata_log_entries m  
  ON f.file_path = m.file  
WHERE m.operation = 'DELETE'  
  AND f.partition = 'region=EU'  
  AND f.last_modified < DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY);
```

## Snippet 8

```text
SELECT snapshot_id, committed_at, operation, summary  
FROM "db"."critical_fact_table.snapshots"  
ORDER BY committed_at DESC;
```

## Snippet 9

```text
SELECT ref_name, type, snapshot_id, max_reference_age_in_ms  
FROM "db"."critical_fact_table.refs";
```

## Snippet 10

```text
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
    ])                                                   #A

                                                 #B

with DAG('iceberg_retention_policy',  
         default_args=default_args,  
         schedule_interval='@daily',  
         catchup=False) as dag:

    expire = PythonOperator(  
        task_id='expire_old_snapshots',  
        python_callable=expire_snapshots  
    )

    expire >> cleanup                                   #B
```

## Snippet 11

```text
from openlineage.spark import OpenLineageSparkListener  
from pyspark.sql import SparkSession

spark = SparkSession.builder \  
    .appName("iceberg-lineage-tracking") \  
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \  
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \  
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://my-bucket/warehouse") \  
    .config("spark.extraListeners", "openlineage.spark.agent.OpenLineageSparkListener") \  #A  
    .config("openlineage.transport.type", "http") \  
    .config("openlineage.transport.url", "http://openlineage-collector:5000") \              #B  
    .getOrCreate()

df = spark.read.table("my_catalog.db.critical_fact_table")  
df.filter("region = 'US'").writeTo("my_catalog.db.reporting_fact_table").overwrite() #C
```

## Snippet 12

```text
SELECT   
  snapshot_id,   
  operation,       --#A  
  committed_at,    --#B  
  user_id          --#C  
FROM my_catalog.db.table.snapshots  
ORDER BY committed_at DESC;
```

## Snippet 13

```text
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

## Snippet 14

```text
SELECT   
  COUNT(*) AS commits,  
  MAX(committed_at) AS last_commit,  
  MIN(committed_at) AS first_commit  
FROM my_catalog.db.sales.snapshots  
WHERE committed_at > current_date - INTERVAL '30' DAY
```

## Snippet 15

```text
CALL my_catalog.system.register_table(  
  table => 'recovered_db.recovered_table',  
  metadata_file => 's3://my-bucket/iceberg/metadata/metadata-000123.json'  
);  --#A

CALL my_catalog.system.set_current_snapshot(  
  table => 'recovered_db.recovered_table',  
  snapshot_id => 123  
);  --#B
```

## Snippet 16

```text
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
        """,  #A  
        spark_conn_id='spark_default'  
    )

    expire = SparkSqlOperator(  
        task_id='expire_newer_snapshots',  
        sql="""  
            CALL my_catalog.system.expire_snapshots(  
              table => 'prod.sales',  
              older_than => TIMESTAMP '2025-09-10 00:00:00'  
            )  
        """,  #B  
        spark_conn_id='spark_default'  
    )

    rollback >> expire  #C
```

## Snippet 17

```text
SELECT file_path  
FROM my_catalog.prod.sales.manifests  
WHERE NOT EXISTS (  
  SELECT 1  
  FROM my_catalog.prod.sales.files  
  WHERE files.file_path = manifests.file_path  
);  --#A
```

## Snippet 18

```text
CALL nessie.catalog.branch(  
  name => 'recovery_zone',  
  from_ref => 'main'  
);  --#A

CALL nessie.catalog.fast_forward(  
  table => 'prod.sales',  
  branch => 'recovery_zone',  
  to => 'main'  
);  --#B
```

