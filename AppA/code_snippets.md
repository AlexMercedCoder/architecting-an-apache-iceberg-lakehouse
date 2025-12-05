# Chapter Code Snippets

In this file you'll find the code snippets in this chapter

```sql
SELECT * FROM db.table.history;
SELECT * FROM db.table.snapshots;
SELECT * FROM db.table.entries;
```

```sql
SELECT * FROM TABLE(table_history('my_table'));
SELECT * FROM TABLE(table_snapshot('my_table'));
SELECT * FROM TABLE(table_files('my_table'));
```

```sql
--Using Spark:
SELECT * FROM db.table.history;
--Using Dremio:
SELECT * FROM TABLE(table_history('my_table'));
```

```sql
SELECT
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['spark.app.id']
FROM db.table.history h
JOIN db.table.snapshots s ON h.snapshot_id = s.snapshot_id
ORDER BY h.made_current_at;
```

```sql
--Using Spark:
SELECT * FROM db.table.snapshots;
--Using Dremio:
SELECT * FROM TABLE(table_snapshot('my_table'));
```

```sql
SELECT snapshot_id, operation, committed_at
FROM db.table.snapshots
WHERE operation = 'delete';
```

```sql
SELECT committed_at, operation, snapshot_id, summary['spark.app.id']
FROM db.table.snapshots
ORDER BY committed_at DESC;
```

```sql
--Using Spark:
SELECT * FROM db.table.metadata_log_entries;
--Using Dremio:
SELECT * FROM TABLE(table_history('my_table'));
```

```sql
SELECT l.timestamp, l.file, s.operation
FROM db.table.metadata_log_entries l
LEFT JOIN db.table.snapshots s
  ON l.latest_snapshot_id = s.snapshot_id;
```

```sql
--Using Spark:
SELECT * FROM db.table.manifests;
--Using Dremio:
SELECT * FROM TABLE(table_manifests('db.table'));
```

```sql
SELECT path, length, added_snapshot_id
FROM db.table.manifests
ORDER BY length DESC
LIMIT 10;
```

```sql
-- Using Spark: 
SELECT * FROM db.table.partitions;
-- Using Dremio:
SELECT * FROM TABLE(table_partitions('db.table'));
```

```sql
SELECT partition, file_count
FROM db.table.partitions
ORDER BY file_count DESC
LIMIT 5;
```

```sql
-- Using Spark:
SELECT * FROM db.table.files;
-- Using Dremio:
SELECT * FROM TABLE(table_files('db.table'));
```
```sql
SELECT file_path, file_size_in_bytes
FROM db.table.files
WHERE file_size_in_bytes < 134217728;
```

```sql
SELECT file_path, content
FROM db.table.files
WHERE content != 0;
```

```sql
SELECT readable_metrics
FROM db.table.files
WHERE file_path LIKE '%parquet';
```

```sql
-- Using Spark:
SELECT * FROM db.table.manifests;
-- Using Dremio:
SELECT * FROM TABLE(table_manifests('db.table'));
```

```sql
SELECT COUNT(*), SUM(length) AS total_manifest_bytes
FROM db.table.manifests;
```

```sql
SELECT partition_summaries
FROM db.table.manifests
WHERE content = 0;
```

```sql
SELECT s.committed_at, m.path, m.length
FROM db.table.snapshots s
JOIN db.table.manifests m ON s.snapshot_id = m.added_snapshot_id;
```

```sql
--Using Spark
SELECT * FROM db.table.partitions;
---Using Dremio:
SELECT * FROM TABLE(table_partitions('db.table'));
```

```sql
SELECT partition, record_count, total_data_file_size_in_bytes
FROM db.table.partitions
ORDER BY record_count DESC
LIMIT 10;
```

```sql
SELECT partition, file_count
FROM db.table.partitions
WHERE file_count > 50;
```

```sql
SELECT partition, position_delete_record_count
FROM db.table.partitions
WHERE position_delete_record_count > 0;
```

```sql
-- In Spark:
SELECT * FROM db.table.position_deletes;
-- In Dremio:
SELECT * FROM TABLE(table('db.table').position_deletes);
```

```sql
SELECT COUNT(*) FROM db.table.position_deletes;
```

```sql
SELECT partition, COUNT(*) AS delete_count
FROM db.table.position_deletes
GROUP BY partition
ORDER BY delete_count DESC;
```

```sql
-- In Spark:
SELECT * FROM db.table.all_data_files;
-- In Dremio:
SELECT * FROM TABLE(table_all_data_files('db.table'));
```

```sql
SELECT
  partition,
  MAX(file_size_in_bytes) AS max_size,
  MIN(file_size_in_bytes) AS min_size
FROM db.table.all_data_files
GROUP BY partition;
```

```sql
-- In Spark:
SELECT * FROM db.table.all_delete_files;
-- In Dremio:
SELECT * FROM TABLE(table_all_delete_files('db.table'));
```

```sql
SELECT COUNT(*) AS num_delete_files,
       SUM(record_count) AS total_deleted_rows
FROM db.table.all_delete_files;
```

```sql
-- In Spark:
SELECT * FROM db.table.all_entries;
-- In Dremio:
SELECT * FROM TABLE(table_all_entries('db.table'));
```

```sql
SELECT e.status, e.snapshot_id, s.committed_at, e.data_file.file_path
FROM db.table.all_entries e
JOIN db.table.snapshots s ON e.snapshot_id = s.snapshot_id
ORDER BY s.committed_at;
```

```sql
-- In Spark:
SELECT * FROM db.table.all_manifests;
-- In Dremio:
SELECT * FROM TABLE(table_all_manifests('db.table'));
```

```sql
--In Spark:
SELECT * FROM db.table.refs;
```

```sql
SELECT COUNT(*) AS total_files,
       SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_files
FROM db.table.files;
```

```sql
SELECT COUNT(*) AS snapshots_last_24h
FROM db.table.snapshots
WHERE committed_at > CURRENT_TIMESTAMP() - INTERVAL 1 DAY;
```