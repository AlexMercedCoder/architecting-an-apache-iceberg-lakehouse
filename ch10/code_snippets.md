# Chapter 10 Code Snippets

In this file you'll find the code snippets extracted from the chapter's manuscript.

## Snippet 1

```text
CALL catalog_name.system.rewrite_data_files('db.sales');
```

## Snippet 2

```text
CALL catalog_name.system.rewrite_data_files(  
    table => 'db.sales',  
    strategy => 'sort',  
    sort_order => 'zorder(customer_id, product_id)'  
);
```

## Snippet 3

```text
OPTIMIZE TABLE demo.example_table
```

## Snippet 4

```text
CALL catalog_name.system.rewrite_manifests('db.sales');
```

## Snippet 5

```text
CALL catalog_name.system.rewrite_data_files(  
  table => 'db.sales',  
  options => map('target-file-size-bytes', '268435456')  
);
```

## Snippet 6

```text
OPTIMIZE TABLE db.sales   
  REWRITE DATA USING BIN_PACK (TARGET_FILE_SIZE_MB = 256);
```

## Snippet 7

```text
CALL catalog_name.system.rewrite_data_files(  
  table => 'db.sales',  
  options => map(  
    'min-file-size-bytes', '134217728',        -- 128 MB  
    'max-file-size-bytes', '1073741824',       -- 1 GB  
    'min-input-files', '5'  
  )  
);
```

## Snippet 8

```text
OPTIMIZE TABLE db.sales  
  REWRITE DATA USING BIN_PACK (  
    MIN_FILE_SIZE_MB = 128,  
    MAX_FILE_SIZE_MB = 1024,  
    MIN_INPUT_FILES = 5  
  );
```

## Snippet 9

```text
CALL catalog_name.system.rewrite_data_files(  
  table => 'db.sales',  
  where => "region = 'us-west'"  
);
```

## Snippet 10

```text
where => "region = 'us-west' AND sale_date >= '2024-01-01'"
```

## Snippet 11

```text
OPTIMIZE TABLE db.sales  
  REWRITE DATA USING BIN_PACK  
  FOR PARTITIONS region = 'us-west'
```

## Snippet 12

```text
CALL catalog_name.system.expire_snapshots(  
  table => 'db.sales',  
  older_than => TIMESTAMP '2024-06-01 00:00:00.000',  
  retain_last => 20  
);
```

## Snippet 13

```text
VACUUM TABLE s3.sales EXPIRE SNAPSHOTS older_than '2024-06-01 00:00:00.000' retain_last 20;
```

