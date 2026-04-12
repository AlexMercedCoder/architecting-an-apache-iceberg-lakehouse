# Chapter B Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
pip install pyiceberg[sql] pyarrow
```

## Snippet 2

```text
from pyiceberg.catalog import load_catalog  
from pyarrow import parquet as pq

catalog = load_catalog(  
    "default",  
    type="sql",  
    uri="sqlite:///demo.db",  
    warehouse="file:///tmp/warehouse"  
)  #A

catalog.create_namespace("default")  #B

table = catalog.create_table(  
    "default.taxi_data",  
    schema=pq.read_schema("trips.parquet")  
)  #C

arrow_table = pq.read_table("trips.parquet")  
table.append(arrow_table)  #D

result = table.scan().to_arrow()  #E

print(len(result))  #F
```

## Snippet 3

```text
update = table.update_schema()  
update.add_column("pickup_borough", "string").commit()  #A

new_data = pq.read_table("updated_trips.parquet")  
table.append(new_data)  #B
```

## Snippet 4

```text
from pyiceberg.expressions import EqualTo

overwrite = table.overwrite(where=EqualTo("pickup_date", "2023-01-01"))  
overwrite.add_file("new_data.parquet").commit()  #A
```

## Snippet 5

```text
from pyiceberg.expressions import GreaterThan

expensive_trips = table.scan(  
    row_filter=GreaterThan("total_amount", 100)  
).to_arrow()  #A

print(expensive_trips.to_pandas().head())  #B
```

## Snippet 6

```text
pip install polars pyiceberg pyarrow
```

## Snippet 7

```text
import polars as pl

lazy_df = pl.scan_iceberg(  
    "s3://data-lake/warehouse/trips/metadata.json",  
    storage_options={"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}  
)  #A

df = lazy_df.collect()  #B

print(df.describe())  #C
```

## Snippet 8

```text
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

## Snippet 9

```text
# Create a small Polars DataFrame  
new_data = pl.DataFrame({  
    "trip_id": [101, 102, 103],  
    "passenger_count": [1, 2, 3],  
    "trip_distance": [3.5, 5.1, 2.9]  
})  #A

# Append this data to an existing Iceberg table  
new_data.write_iceberg(  
    "default.trips_table",  
    mode="append"  
)  #B
```

## Snippet 10

```text
pip install duckdb
```

## Snippet 11

```text
import duckdb

con = duckdb.connect("iceberg_demo.db")  #A

con.execute("INSTALL iceberg;")  #B

con.execute("LOAD iceberg;")  #C
```

## Snippet 12

```text
result = con.execute("""  
    SELECT vendor_id, trip_distance, total_amount  
    FROM iceberg_scan('/data/warehouse/trips/metadata.json')  
    WHERE trip_distance > 5  
    ORDER BY total_amount DESC  
    LIMIT 10  
""").df()  #A

print(result.head())  #B
```

## Snippet 13

```text
con.execute("""  
    ATTACH 'my_catalog' AS iceberg_catalog (  
        TYPE iceberg,  
        ENDPOINT 'https://catalog-host.com/v1',  
        SECRET 'my_secret_key'  
    );  
""")  #A

tables = con.execute("SHOW TABLES FROM iceberg_catalog.default;").fetchall()  #B

con.execute("""  
    CREATE TABLE iceberg_catalog.default.daily_trips AS  
    SELECT * FROM iceberg_catalog.default.trips WHERE trip_date = '2023-01-01';  
""")  #C
```

## Snippet 14

```text
con.execute("""  
    INSERT INTO iceberg_catalog.default.daily_trips  
    VALUES (1001, 3.2, 15.75), (1002, 7.1, 28.10);  
""")  #A
```

## Snippet 15

```text
# Query a specific snapshot using a snapshot ID  
historical = con.execute("""  
    SELECT COUNT(*) AS trips_count  
    FROM iceberg_scan(  
        '/data/warehouse/trips/metadata.json',  
        snapshot_id := '9182736455'  
    );  
""").df()  #A

print(historical)
```

## Snippet 16

```text
pip install daft pyiceberg pyarrow s3fs
```

## Snippet 17

```text
import daft  
from pyiceberg.catalog import load_catalog

catalog = load_catalog(  
    "default",  
    type="sql",  
    uri="sqlite:///demo.db",  
    warehouse="file:///tmp/warehouse"  
)  #A

table = catalog.load_table("default.taxi_data")  #B

df = daft.read_iceberg(table)  #C

df.show(5)  #D
```

## Snippet 18

```text
filtered = (  
    df.filter(df["total_amount"] > 100)        #A  
      .groupby("passenger_count")              #B  
      .agg({"trip_distance": ["mean"]})        #C  
)

filtered.show()                                #D
```

## Snippet 19

```text
# Convert Daft DataFrame to Arrow Table  
arrow_table = df.to_arrow_table()  #A

# Convert Arrow Table to Polars DataFrame  
import polars as pl  
polars_df = pl.from_arrow(arrow_table)  #B
```

## Snippet 20

```text
# Write Daft DataFrame to Parquet files  
df.write_parquet("s3://data-lake/cleaned/trips/")  #A

# Register exported data as a new Iceberg table  
catalog.create_table(  
    "default.cleaned_trips",  
    schema=df.schema().to_arrow(),  
    location="s3://data-lake/cleaned/trips/"  
)  #B
```

## Snippet 21

```text
pip install pyodbc pyarrow
```

## Snippet 22

```text
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

## Snippet 23

```text
from pyarrow import flight

client = flight.FlightClient("grpc+tls://data.dremio.cloud:443")  #A

headers = [(b"authorization", f"bearer {token}".encode())]  #B

sql = 'SELECT trip_distance, total_amount FROM "Samples"."NYC_Trips" WHERE total_amount > 100'  #C

flight_info = client.get_flight_info(  
    flight.FlightDescriptor.for_command(sql),  
    flight.FlightCallOptions(headers=headers)  
)  #D

reader = client.do_get(flight_info.endpoints[0].ticket, flight.FlightCallOptions(headers=headers))  #E  
arrow_table = reader.read_all()  #F

print(arrow_table.to_pandas().head())  #G
```

## Snippet 24

```text
cursor.execute("""  
    CREATE TABLE "Samples"."daily_summary" AS  
    SELECT passenger_count, AVG(total_amount) AS avg_total  
    FROM "Samples"."NYC_Trips"  
    GROUP BY passenger_count;  
""")  #A

cursor.execute("""  
    INSERT INTO "Samples"."daily_summary"  
    VALUES (4, 32.75), (5, 45.12);  
""")  #B

conn.commit()  #C
```

## Snippet 25

```text
pip install bauplan
```

## Snippet 26

```text
import bauplan

client = bauplan.Client(api_key="my_api_key")  #A

client.create_branch("dev", from_ref="main")  #B

client.create_namespace("default", branch="dev")  #C
```

## Snippet 27

```text
client.create_table(  
    table="trips",  
    search_uri="s3://data-lake/trips/*.parquet",  
    namespace="default",  
    branch="dev",  
    replace=True  
)  #A

client.import_data(  
    table="trips",  
    search_uri="s3://data-lake/trips/*.parquet",  
    namespace="default",  
    branch="dev"  
)  #B

arrow_table = client.scan(  
    table="trips",  
    ref="dev",  
    namespace="default",  
    columns=["passenger_count", "total_amount"]  
)  #C

print(arrow_table.to_pandas().head())  #D
```

## Snippet 28

```text
assert arrow_table["passenger_count"].null_count == 0  #A

client.merge_branch(source_ref="dev", into_branch="main")  #B

client.delete_branch("dev")  #C
```

## Snippet 29

```text
client.update_schema(  
    table="trips",  
    namespace="default",  
    branch="dev",  
    add_columns=[{"name": "pickup_zone", "type": "string"}]  
)  #A
```

## Snippet 30

```text
pip install spicepy
```

## Snippet 31

```text
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

## Snippet 32

```text
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

## Snippet 33

```text
datasets:  
  - name: trips  
    from: iceberg:https://catalog.mycompany.com/v1/namespaces/default/tables/trips  #A
```

## Snippet 34

```text
from pyiceberg.catalog import load_catalog  
from pyarrow import parquet as pq

# Initialize a local Iceberg catalog for testing  
catalog = load_catalog(  
    "default",  
    type="sql",  
    uri="sqlite:///demo.db",  
    warehouse="file:///tmp/warehouse"  
)  #A

# Create a new Iceberg table from a Parquet dataset  
table = catalog.create_table(  
    "default.sales",  
    schema=pq.read_schema("sales_data.parquet")  
)  #B

# Read Parquet file as Arrow Table and append to Iceberg  
data = pq.read_table("sales_data.parquet")  
table.append(data)  #C

# Perform incremental overwrite for a given partition  
from pyiceberg.expressions import EqualTo  
overwrite = table.overwrite(where=EqualTo("region", "US"))  #D  
overwrite.add_file("new_sales_data.parquet").commit()  #E

# Query the table with a filter for analytics  
from pyiceberg.expressions import GreaterThan  
results = table.scan(row_filter=GreaterThan("amount", 1000)).to_arrow()  #F  
print(results.to_pandas().head())  #G
```

