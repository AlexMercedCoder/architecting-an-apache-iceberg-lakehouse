# Chapter 3 Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
docker --version

![][image3]
```

## Snippet 2

```text
version: "3" #A

services: #B

  nessie: #C

    image: projectnessie/nessie:latest

    container_name: nessie

    networks:

      lakehouse-net:

    ports:

      - 19120:19120
```

## Snippet 3

```text
minio: #A

    image: minio/minio:latest

    container_name: minio

    environment:

      - MINIO_ROOT_USER=admin

      - MINIO_ROOT_PASSWORD=password

      - MINIO_REGION=us-east-1

    networks:

      lakehouse-net:

    ports:

      - 9001:9001

      - 9000:9000

    command: ["server", "/data", "--console-address", ":9001"]
```

## Snippet 4

```text
dremio: #A

    image: dremio/dremio-oss:latest

    container_name: dremio

    networks:

      lakehouse-net:

    ports:

      - 9047:9047

      - 31010:31010

      - 32010:32010
```

## Snippet 5

```text
spark: #A

    image: alexmerced/spark35nb:latest

    container_name: spark

    networks:

      lakehouse-net:

    ports:

      - 8080:8080  # Master Web UI

      - 7077:7077  # Master Port

      - 8888:8888  # Jupyter Notebook

    environment:

      - AWS_ACCESS_KEY_ID=admin

      - AWS_SECRET_ACCESS_KEY=password

      - AWS_DEFAULT_REGION=us-east-1

      - AWS_REGION=us-east-1
```

## Snippet 6

```text
postgres: #A

    image: postgres:latest

    container_name: postgres

    environment:

      POSTGRES_DB: mydb

      POSTGRES_USER: myuser

      POSTGRES_PASSWORD: mypassword

    networks:

      lakehouse-net:

    ports:

      - "5435:5432"
```

## Snippet 7

```text
superset: #A

    image: alexmerced/dremio-superset

    container_name: superset

    networks:

      lakehouse-net:

    ports:

      - 8088:8088

Networks: #B

  lakehouse-net:
```

## Snippet 8

```text
docker-compose up -d
```

## Snippet 9

```text
docker ps
```

## Snippet 10

```text
CONTAINER ID   IMAGE                         STATUS         PORTS                    

xxxxxxxxxx     projectnessie/nessie:latest   Up 5 minutes   19120/tcp                  

xxxxxxxxxx     minio/minio:latest            Up 5 minutes   9000-9001/tcp              

xxxxxxxxxx     dremio/dremio-oss:latest      Up 5 minutes   9047/tcp, 31010-32010/tcp  

xxxxxxxxxx     alexmerced/spark35notebook    Up 5 minutes   7077/tcp, 8888/tcp        

xxxxxxxxxx     postgres:latest               Up 5 minutes   5435->5432/tcp            

xxxxxxxxxx     alexmerced/dremio-superset    Up 5 minutes   8088/tcp
```

## Snippet 11

```text
docker exec -it postgres psql -U myuser mydb
```

## Snippet 12

```text
CREATE TABLE fashion_sales (

    id SERIAL PRIMARY KEY,

    product_name VARCHAR(255),

    category VARCHAR(50),

    sales_amount DECIMAL(10, 2),

    sales_date DATE,

    store_location VARCHAR(100),

    customer_age_group VARCHAR(50),

    campaign_name VARCHAR(100)

); #A

INSERT INTO fashion_sales (product_name, category, sales_amount, sales_date, store_location, customer_age_group, campaign_name)

VALUES

    ('Slim Fit Jeans', 'Denim', 89.99, '2024-03-01', 'New York', '18-24', 'Spring Launch'),

    ('Leather Jacket', 'Outerwear', 249.99, '2024-03-01', 'Los Angeles', '25-34', 'Spring Launch'),

    ('Graphic T-Shirt', 'Tops', 39.99, '2024-03-02', 'Chicago', '18-24', 'March Madness'),

    ('Summer Dress', 'Dresses', 129.99, '2024-03-03', 'New York', '35-44', 'March Madness'),

    ('Casual Sneakers', 'Footwear', 99.99, '2024-03-03', 'Los Angeles', '25-34', 'Spring Launch'); #B
```

## Snippet 13

```text
docker network ls
```

## Snippet 14

```text
docker network inspect <network_name>
```

## Snippet 15

```text
import pyspark 

from pyspark.sql import SparkSession #A

## DEFINE VARIABLES #B

CATALOG_URI = "http://nessie:19120/api/v1"

WAREHOUSE = "s3://warehouse/"

STORAGE_URI = "http://<minio_ip>:9000"
```

## Snippet 16

```text
conf = (

    pyspark.SparkConf()

        .setAppName('Iceberg Ingestion')

        .set('spark.jars.packages', 

             'org.postgresql:postgresql:42.7.3,'

             'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'

             'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,'

             'software.amazon.awssdk:bundle:2.24.8,'

             'software.amazon.awssdk:url-connection-client:2.24.8')

        .set('spark.sql.extensions', 

             'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'

             'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')

        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')

        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)

        .set('spark.sql.catalog.nessie.ref', 'main')

        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')

        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')

        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)

        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)

        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')

) #A
```

## Snippet 17

```text
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("Spark Running")
```

## Snippet 18

```text
# Define the JDBC connection properties

jdbc_url = "jdbc:postgresql://postgres:5432/mydb" #A

properties = {

    "user": "myuser",

    "password": "mypassword",

    "driver": "org.postgresql.Driver"

} #B

# Read the sales_data table from Postgres into a Spark DataFrame

sales_df = spark.read.jdbc(url=jdbc_url, table="fashion_sales", properties=properties) #C

# Show the first few rows of the dataset

sales_df.show() #D
```

## Snippet 19

```text
#Create a namespace

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales;") #A

# Write the DataFrame to an Iceberg table in the Nessie catalog

sales_df.writeTo("nessie.sales.fashion_sales").createOrReplace() #B

# Verify that the data was written to Iceberg by reading the table

spark.read.table("nessie.sales.fashion_sales").show() #C
```

## Snippet 20

```text
SELECT * FROM nessie.sales.fashion_sales;
```

## Snippet 21

```text
SELECT * FROM TABLE( table_snapshot( 'nessie.sales.fashion_sales' ) )
```

## Snippet 22

```text
SELECT * FROM nessie.sales.sales_data AT SNAPSHOT <snapshot_id>;
```

## Snippet 23

```text
SELECT * FROM nessie.sales.sales_data AT TIMESTAMP <timestamp>;
```

## Snippet 24

```text
docker-compose up -d superset
```

## Snippet 25

```text
docker exec -it superset superset init
```

## Snippet 26

```text
docker compose down -v
```

