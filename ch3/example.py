import pyspark
from pyspark.sql import SparkSession

## DEFINE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3://warehouse/"
STORAGE_URI = "http://minio:9000"

## CONFIGURE SPARK SESSION
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
)

## START SPARK SESSION
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# Define the JDBC connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

# Read the sales_data table from Postgres into a Spark DataFrame
sales_df = spark.read.jdbc(url=jdbc_url, table="fashion_sales", properties=properties)

# Show the first few rows of the dataset
sales_df.show()

#Create a namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales;")

# Write the DataFrame to an Iceberg table in the Nessie catalog
sales_df.writeTo("nessie.sales.fashion_sales").createOrReplace()

# Verify that the data was written to Iceberg by reading the table
spark.read.table("nessie.sales.fashion_sales").show()
