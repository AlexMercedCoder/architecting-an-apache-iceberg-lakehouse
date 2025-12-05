# Chapter Code Snippets

In this file you'll find the code snippets in this chapter

```python
pip install pyiceberg[sql] pyarrow
```

```python
from pyiceberg.catalog import load_catalog
from pyarrow import parquet as pq

catalog = load_catalog(
    "default",
    type="sql",
    uri="sqlite:///demo.db",
    warehouse="file:///tmp/warehouse"
)  #A

catalog.create_namespace("default")

table = catalog.create_table(
    "default.taxi_data",
    schema=pq.read_schema("trips.parquet")
)  #C

arrow_table = pq.read_table("trips.parquet")
table.append(arrow_table)

result = table.scan().to_arrow()

print(len(result))
```

```python
update = table.update_schema()
update.add_column("pickup_borough", "string").commit()

new_data = pq.read_table("updated_trips.parquet")
table.append(new_data)
```

```python
from pyiceberg.expressions import EqualTo

overwrite = table.overwrite(where=EqualTo("pickup_date", "2023-01-01"))
overwrite.add_file("new_data.parquet").commit()  #A
```

```python
from pyiceberg.expressions import GreaterThan

expensive_trips = table.scan(
    row_filter=GreaterThan("total_amount", 100)
).to_arrow()

print(expensive_trips.to_pandas().head())
```

```shell
pip install polars pyiceberg pyarrow
```

```python
import polars as pl

lazy_df = pl.scan_iceberg(
    "s3://data-lake/warehouse/trips/metadata.json",
    storage_options={"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}
)

df = lazy_df.collect()

print(df.describe())
```

```python
summary = (
    lazy_df
    .filter(pl.col("passenger_count") > 1)  #A
    .group_by("passenger_count")            #B
    .agg(pl.col("trip_distance").mean().alias("avg_distance"))  #C
    .sort("passenger_count")                #D
    .collect()                              #E
)

print(summary)
```

```python
# Create a small Polars DataFrame
new_data = pl.DataFrame({
    "trip_id": [101, 102, 103],
    "passenger_count": [1, 2, 3],
    "trip_distance": [3.5, 5.1, 2.9]
})

# Append this data to an existing Iceberg table
new_data.write_iceberg(
    "default.trips_table",
    mode="append"
)
```

```shell
pip install duckdb
```

```python
import duckdb

con = duckdb.connect("iceberg_demo.db")  #A

con.execute("INSTALL iceberg;")  #B

con.execute("LOAD iceberg;")  #C
```

```python
result = con.execute("""
    SELECT vendor_id, trip_distance, total_amount
    FROM iceberg_scan('/data/warehouse/trips/metadata.json')
    WHERE trip_distance > 5
    ORDER BY total_amount DESC
    LIMIT 10
""").df()

print(result.head())
```

```python
con.execute("""
    ATTACH 'my_catalog' AS iceberg_catalog (
        TYPE iceberg,
        ENDPOINT 'https://catalog-host.com/v1',
        SECRET 'my_secret_key'
    );
""")

tables = con.execute("SHOW TABLES FROM iceberg_catalog.default;").fetchall()

con.execute("""
    CREATE TABLE iceberg_catalog.default.daily_trips AS
    SELECT * FROM iceberg_catalog.default.trips WHERE trip_date = '2023-01-01';
""")
```

```python
con.execute("""
    INSERT INTO iceberg_catalog.default.daily_trips
    VALUES (1001, 3.2, 15.75), (1002, 7.1, 28.10);
""")
```

```python
historical = con.execute("""
    SELECT COUNT(*) AS trips_count
    FROM iceberg_scan(
        '/data/warehouse/trips/metadata.json',
        snapshot_id := '9182736455'
    );
""").df()

print(historical)
```

```shell
pip install daft pyiceberg pyarrow s3fs
```

```python
import daft
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    type="sql",
    uri="sqlite:///demo.db",
    warehouse="file:///tmp/warehouse"
)

table = catalog.load_table("default.taxi_data")

df = daft.read_iceberg(table)

df.show(5)
```

```python
filtered = (
    df.filter(df["total_amount"] > 100) 
      .groupby("passenger_count")   
      .agg({"trip_distance": ["mean"]})    
)

filtered.show()
```

```python
# Convert Daft DataFrame to Arrow Table
arrow_table = df.to_arrow_table()

# Convert Arrow Table to Polars DataFrame
import polars as pl
polars_df = pl.from_arrow(arrow_table)
```

```python
# Write Daft DataFrame to Parquet files
df.write_parquet("s3://data-lake/cleaned/trips/")

# Register exported data as a new Iceberg table
catalog.create_table(
    "default.cleaned_trips",
    schema=df.schema().to_arrow(),
    location="s3://data-lake/cleaned/trips/"
)
```

```shell
pip install pyodbc pyarrow
```

```python
import pyodbc

conn = pyodbc.connect(
    "Driver={Dremio Connector 64-bit};"
    "ConnectionType=Direct;"
    "HOST=demo.dremio.cloud;"
    "PORT=443;"
    "AuthenticationType=Plain;"
    "UID=my_username;"
    "PWD=my_token;"
    "SSL=1"
)  #A

cursor = conn.cursor()  #B

cursor.execute('SELECT * FROM "Samples"."NYC_Trips" LIMIT 5;')  #C

rows = cursor.fetchall()  #D
for row in rows:
    print(row)
```

```python
from pyarrow import flight

client = flight.FlightClient("grpc+tls://data.dremio.cloud:443") 

headers = [(b"authorization", f"bearer {token}".encode())] 

sql = 'SELECT trip_distance, total_amount FROM "Samples"."NYC_Trips" WHERE total_amount > 100' 

flight_info = client.get_flight_info(
    flight.FlightDescriptor.for_command(sql),
    flight.FlightCallOptions(headers=headers)
) 

reader = client.do_get(flight_info.endpoints[0].ticket, flight.FlightCallOptions(headers=headers))
arrow_table = reader.read_all()

print(arrow_table.to_pandas().head())
```

```python
cursor.execute("""
    CREATE TABLE "Samples"."daily_summary" AS
    SELECT passenger_count, AVG(total_amount) AS avg_total
    FROM "Samples"."NYC_Trips"
    GROUP BY passenger_count;
""")

cursor.execute("""
    INSERT INTO "Samples"."daily_summary"
    VALUES (4, 32.75), (5, 45.12);
""")

conn.commit()
```
```
pip install bauplan
```

```python
client = bauplan.Client(api_key="my_api_key")

client.create_branch("dev", from_ref="main")

client.create_namespace("default", branch="dev")
```

```python
client.create_table(
    table="trips",
    search_uri="s3://data-lake/trips/*.parquet",
    namespace="default",
    branch="dev",
    replace=True
)

client.import_data(
    table="trips",
    search_uri="s3://data-lake/trips/*.parquet",
    namespace="default",
    branch="dev"
)

arrow_table = client.scan(
    table="trips",
    ref="dev",
    namespace="default",
    columns=["passenger_count", "total_amount"]
)

print(arrow_table.to_pandas().head())
```

```python
assert arrow_table["passenger_count"].null_count == 0

client.merge_branch(source_ref="dev", into_branch="main")

client.delete_branch("dev")
```

```python
client.update_schema(
    table="trips",
    namespace="default",
    branch="dev",
    add_columns=[{"name": "pickup_zone", "type": "string"}]
)
```

```shell
pip install spicepy
```

```python
from spicepy import Client

client = Client()  #A

result = client.query("""
    SELECT pickup_borough, AVG(total_amount) AS avg_fare
    FROM iceberg.default.trips
    WHERE total_amount > 50
    GROUP BY pickup_borough
    ORDER BY avg_fare DESC
""")  #B

df = result.read_pandas()  #C

print(df.head())  #D
```

```python
joined = client.query("""
    SELECT i.trip_id, i.total_amount, p.driver_rating
    FROM iceberg.default.trips AS i
    JOIN postgres.analytics.driver_ratings AS p
    ON i.driver_id = p.driver_id
    WHERE i.total_amount > 100
""")  #A

joined_df = joined.read_pandas()  #B

print(joined_df.describe())  #C
```

```python
from pyiceberg.catalog import load_catalog
from pyarrow import parquet as pq

# Initialize a local Iceberg catalog for testing
catalog = load_catalog(
    "default",
    type="sql",
    uri="sqlite:///demo.db",
    warehouse="file:///tmp/warehouse"
)

# Create a new Iceberg table from a Parquet dataset
table = catalog.create_table(
    "default.sales",
    schema=pq.read_schema("sales_data.parquet")
)

# Read Parquet file as Arrow Table and append to Iceberg
data = pq.read_table("sales_data.parquet")
table.append(data)

# Perform incremental overwrite for a given partition
from pyiceberg.expressions import EqualTo
overwrite = table.overwrite(where=EqualTo("region", "US"))
overwrite.add_file("new_sales_data.parquet").commit()

# Query the table with a filter for analytics
from pyiceberg.expressions import GreaterThan
results = table.scan(row_filter=GreaterThan("amount", 1000)).to_arrow()
print(results.to_pandas().head())
```