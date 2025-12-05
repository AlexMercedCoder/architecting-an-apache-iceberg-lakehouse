# Chapter Code Snippets

In this file you'll find the code snippets in this chapter

```sql
CALL catalog_name.system.rewrite_data_files('db.sales');
```

```sql
CALL catalog_name.system.rewrite_data_files(
    table => 'db.sales',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, product_id)'
);
```

```sql
OPTIMIZE TABLE demo.example_table
  REWRITE DATA USING BIN_PACK (TARGET_FILE_SIZE_MB = 512);
```

```sql
CALL catalog_name.system.rewrite_manifests('db.sales');
```

```sql
CALL catalog_name.system.rewrite_data_files(
  table => 'db.sales',
  options => map('target-file-size-bytes', '268435456')
);
```

```sql
OPTIMIZE TABLE db.sales 
  REWRITE DATA USING BIN_PACK (TARGET_FILE_SIZE_MB = 256);
```

```sql
CALL catalog_name.system.rewrite_data_files(
  table => 'db.sales',
  options => map(
    'min-file-size-bytes', '134217728',        -- 128 MB
    'max-file-size-bytes', '1073741824',       -- 1 GB
    'min-input-files', '5'
  )
);
```

```sql
CALL catalog_name.system.rewrite_data_files(
  table => 'db.sales',
  where => "region = 'us-west'"
);
```

```sql
OPTIMIZE TABLE db.sales
  REWRITE DATA USING BIN_PACK
  FOR PARTITIONS region = 'us-west'
```

```sql
CALL catalog_name.system.expire_snapshots(
  table => 'db.sales',
  older_than => TIMESTAMP '2024-06-01 00:00:00.000',
  retain_last => 20
);
```

```sql
VACUUM TABLE s3.sales EXPIRE SNAPSHOTS older_than '2024-06-01 00:00:00.000' retain_last 20;
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-data-lake-bucket",
        "arn:aws:s3:::your-data-lake-bucket/*"
      ]
    }
  ]
}
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-data-lake-bucket",
        "arn:aws:s3:::your-data-lake-bucket/*"
      ]
    }
  ]
}
```

```shell
polaris privileges \
  table \
  grant \
  --catalog prod_catalog \
  --catalog-role catalog_reader \
  --namespace finance \
  --table transactions \
  TABLE_READ_DATA
```

