# Chapter A Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
SELECT * FROM db.table.history;  
SELECT * FROM db.table.snapshots;  
SELECT * FROM db.table.entries;
```

## Snippet 2

```text
SELECT * FROM TABLE(table_history('my_table'));  
SELECT * FROM TABLE(table_snapshot('my_table'));  
SELECT * FROM TABLE(table_files('my_table'));
```

## Snippet 3

```text
SELECT * FROM db.table.history;
```

## Snippet 4

```text
SELECT * FROM TABLE(table_history('my_table'));
```

## Snippet 5

```text
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

## Snippet 6

```text
SELECT * FROM db.table.snapshots;
```

## Snippet 7

```text
SELECT * FROM TABLE(table_snapshot('my_table'));
```

## Snippet 8

```text
SELECT snapshot_id, operation, committed_at  
FROM db.table.snapshots  
WHERE operation = 'delete';
```

## Snippet 9

```text
SELECT committed_at, operation, snapshot_id, summary['spark.app.id']  
FROM db.table.snapshots  
ORDER BY committed_at DESC;
```

## Snippet 10

```text
SELECT * FROM db.table.metadata_log_entries;
```

## Snippet 11

```text
SELECT * FROM TABLE(table_history('my_table'));
```

## Snippet 12

```text
SELECT l.timestamp, l.file, s.operation  
FROM db.table.metadata_log_entries l  
LEFT JOIN db.table.snapshots s  
  ON l.latest_snapshot_id = s.snapshot_id;
```

## Snippet 13

```text
SELECT * FROM TABLE(table_manifests('db.table'));
```

## Snippet 14

```text
SELECT path, length, added_snapshot_id  
FROM db.table.manifests  
ORDER BY length DESC  
LIMIT 10;
```

## Snippet 15

```text
SELECT * FROM db.table.partitions;
```

## Snippet 16

```text
SELECT * FROM TABLE(table_partitions('db.table'));
```

## Snippet 17

```text
SELECT partition, file_count  
FROM db.table.partitions  
ORDER BY file_count DESC  
LIMIT 5;
```

## Snippet 18

```text
SELECT * FROM db.table.files;
```

## Snippet 19

```text
SELECT * FROM TABLE(table_files('db.table'));
```

## Snippet 20

```text
SELECT file_path, file_size_in_bytes  
FROM db.table.files  
WHERE file_size_in_bytes < 134217728;
```

## Snippet 21

```text
SELECT file_path, content  
FROM db.table.files  
WHERE content != 0;
```

## Snippet 22

```text
SELECT readable_metrics  
FROM db.table.files  
WHERE file_path LIKE '%parquet';
```

## Snippet 23

```text
SELECT * FROM db.table.position_deletes;
```

## Snippet 24

```text
SELECT * FROM TABLE(table('db.table').position_deletes);
```

## Snippet 25

```text
SELECT COUNT(*) FROM db.table.position_deletes;
```

## Snippet 26

```text
SELECT partition, COUNT(*) AS delete_count  
FROM db.table.position_deletes  
GROUP BY partition  
ORDER BY delete_count DESC;
```

## Snippet 27

```text
SELECT * FROM db.table.all_data_files;
```

## Snippet 28

```text
SELECT * FROM TABLE(table_all_data_files('db.table'));
```

## Snippet 29

```text
SELECT  
  partition,  
  MAX(file_size_in_bytes) AS max_size,  
  MIN(file_size_in_bytes) AS min_size  
FROM db.table.all_data_files  
GROUP BY partition;
```

## Snippet 30

```text
SELECT * FROM db.table.all_delete_files;
```

## Snippet 31

```text
SELECT * FROM TABLE(table_all_delete_files('db.table'));
```

## Snippet 32

```text
SELECT COUNT(*) AS num_delete_files,  
       SUM(record_count) AS total_deleted_rows  
FROM db.table.all_delete_files;
```

## Snippet 33

```text
SELECT * FROM db.table.all_entries;
```

## Snippet 34

```text
SELECT * FROM TABLE(table_all_entries('db.table'));
```

## Snippet 35

```text
SELECT e.status, e.snapshot_id, s.committed_at, e.data_file.file_path  
FROM db.table.all_entries e  
JOIN db.table.snapshots s ON e.snapshot_id = s.snapshot_id  
ORDER BY s.committed_at;
```

## Snippet 36

```text
SELECT * FROM db.table.all_manifests;
```

## Snippet 37

```text
SELECT * FROM TABLE(table_all_manifests('db.table'));
```

## Snippet 38

```text
SELECT * FROM db.table.refs;
```

## Snippet 39

```text
SELECT COUNT(*) AS total_files,  
       SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_files  
FROM db.table.files;
```

## Snippet 40

```text
SELECT COUNT(*) AS snapshots_last_24h  
FROM db.table.snapshots  
WHERE committed_at > CURRENT_TIMESTAMP() - INTERVAL 1 DAY;
```

