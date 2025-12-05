# Chapter Code Snippets

In this file you'll find the code snippets in this chapter

```scala
import org.apache.spark.sql.SparkSession 
val spark = SparkSession.builder()
	.appName("IcebergAppendExample")
	.config("spark.sql.catalog.my_catalog",
	"org.apache.iceberg.spark.SparkCatalog")
	.config("spark.sql.catalog.my_catalog.type", "hadoop")
	.config("spark.sql.catalog.my_catalog.warehouse",
	"s3a://my-bucket/warehouse")
	.getOrCreate()  
val df = spark.read.json(
	"s3a://input-bucket/new-data.json") 
df.writeTo("my_catalog.db.my_table")
	.append()
```

```scala
df.writeTo("my_catalog.db.my_table")
	.overwritePartitions()
```

```sql
MERGE INTO my_catalog.db.my_table t
    USING my_catalog.db.staging_table s 
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
```

```scala
val inputStream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "input-topic")
  .load()
val parsed = inputStream.selectExpr("CAST(value AS STRING) as json")
  .select(from_json($"json", schema).as("data"))
  .select("data.*")
parsed.writeStream
  .format("iceberg")
  .outputMode("append")
  .option("checkpointLocation", "s3a://checkpoints/my-table/")
  .start("my_catalog.db.my_table")
```

```SQL
CREATE TABLE source_kafka (
  id STRING,
  event_time TIMESTAMP(3),
  data STRING,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'broker:9092',
  'format' = 'json'
); #A
CREATE TABLE sink_iceberg (
  id STRING,
  event_time TIMESTAMP(3),
  data STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'my_catalog',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3a://my-bucket/warehouse',
  'format-version' = '2'
); #B
INSERT INTO sink_iceberg
SELECT * FROM source_kafka; C
```

```java
DataStream<Row> inputStream = env
	.addSource(new FlinkKafkaConsumer<>("events",
	new SimpleStringSchema(), props))
	.map(json -> parseJsonToRow(json));
Table inputTable = tableEnv.fromDataStream(inputStream);
tableEnv.executeSql(
  "CREATE TABLE iceberg_sink (id STRING, data STRING) WITH (...)"
);
inputTable.executeInsert("iceberg_sink");
```