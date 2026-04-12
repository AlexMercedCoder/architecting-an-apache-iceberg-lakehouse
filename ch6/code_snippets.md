# Chapter 6 Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
import org.apache.spark.sql.SparkSession  #A  
val spark = SparkSession.builder()  
	[CA].appName("IcebergAppendExample")  
	[CA].config("spark.sql.catalog.my_catalog",  
	[CA]"org.apache.iceberg.spark.SparkCatalog")  
	[CA].config("spark.sql.catalog.my_catalog.type", "hadoop")  
	[CA].config("spark.sql.catalog.my_catalog.warehouse",  
	[CA]"s3a://my-bucket/warehouse")  
	.getOrCreate()  #B  
val df = spark.read.json(  
	[CA]"s3a://input-bucket/new-data.json")  #C  
df.writeTo("my_catalog.db.my_table")  
	[CA].append() #D
```

## Snippet 2

```text
df.writeTo("my_catalog.db.my_table")  
	.overwritePartitions()
```

## Snippet 3

```text
MERGE INTO my_catalog.db.my_table t

USING my_catalog.db.staging_table s  #A

ON t.id = s.id #B

WHEN MATCHED THEN UPDATE SET * #C

WHEN NOT MATCHED THEN INSERT * #D
```

## Snippet 4

```text
val inputStream = spark.readStream

  .format("kafka")

  .option("kafka.bootstrap.servers", "broker:9092")

  .option("subscribe", "input-topic")

  .load() #A

val parsed = inputStream.selectExpr("CAST(value AS STRING) as json")

  .select(from_json($"json", schema).as("data"))

  .select("data.*") #B

parsed.writeStream

  .format("iceberg")

  .outputMode("append")

  .option("checkpointLocation", "s3a://checkpoints/my-table/")

  .start("my_catalog.db.my_table") #C
```

## Snippet 5

```text
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

SELECT * FROM source_kafka; #C
```

## Snippet 6

```text
DataStream<Row> inputStream = env

	[CA].addSource(new FlinkKafkaConsumer<>("events",

	[CA]new SimpleStringSchema(), props))

	[CA].map(json -> parseJsonToRow(json)); #A

[CA] Table inputTable = tableEnv.fromDataStream(inputStream); #B 

tableEnv.executeSql(

  "CREATE TABLE iceberg_sink (id STRING, data STRING) WITH (...)"

[CA] ); #C 

[CA] inputTable.executeInsert("iceberg_sink"); #D
```

