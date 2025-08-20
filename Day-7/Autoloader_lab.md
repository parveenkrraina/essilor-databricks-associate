# Databricks Autoloader
## Introduction to Autoloader

Autoloader is a Databricks feature designed for efficient, scalable, and incremental ingestion of new data files as they arrive in cloud storage (e.g., ADLS, S3, GCS). It enables processing of massive datasets in a cost-effective way without manual file tracking.

### Key Benefits

- Incrementally and automatically detects new files
- Scalable for large numbers of files
- Supports schema evolution
- Can ingest from multiple cloud providers

## How Autoloader Works

- **Directory Listing Mode:** Default and most efficient for cloud object stores. Lists files and processes only new ones.
- **File Notification Mode:** Integrates with cloud-native notifications for higher performance and reliability.
- **Checkpointing:** Tracks which files have already been processed (using a checkpoint location) to prevent duplicate ingestion.

## Common Use Cases

- Streaming ingestion of files into a Delta Lake table
- Near real-time ETL pipelines
- Data lake ingestion patterns

## Example Scenario Using Sample Files

We'll demonstrate how to use Autoloader to ingest the following files:

- `customer_details.csv`
- `product_catalog.csv`
- `sales.csv`

Assume these files arrive incrementally in a folder such as `/FileStore/data/input/`.

## Step-by-Step Example: Using Autoloader

### 1. Basic Autoloader Setup (CSV Example)

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

input_path = "/FileStore/data/"
checkpoint_path = "/FileStore/data/checkpoints/autoloader_demo/"
output_path = "/FileStore/data/out"
schema_location = "/FileStore/data/schema/autoloader_demo/"

sales_schema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", StringType()),
    StructField("OrderDate", StringType()),
    StructField("CustomerID", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", DoubleType()),
    StructField("TaxAmount", DoubleType()),
])

sales_df = (
  spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", True)
      .option("cloudFiles.schemaLocation", schema_location)
      .schema(sales_schema)
      .load(input_path)
)

query = (
  sales_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(output_path)
)

```

### 2. Autoloader with Schema Evolution
If you expect the schema to change (columns added/removed), enable schema evolution:

```python
sales_df = (
    spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("cloudFiles.schemaLocation", checkpoint_path + "/schema/")
            .load(input_path)
)
```

### 3. Autoloader with Multiple File Types

If your input folder has mixed file types (e.g., CSV and JSON):

```python
sales_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.schemaLocation", "/mnt/schema/sales")
    .load(input_path)
```

## 4. Hands-On Exercise

**Exercise:**

1. Place all sample files in `/FileStore/data/input/`.
2. Run the above notebook cells to create a Delta table from the ingested data.
3. Monitor the stream, add a new file, and confirm that Autoloader picks it up automatically.
4. Query the Delta table in a new cell:

```python
spark.read.format("delta").load(output_path).show()
```

## Additional Tips

- **File Naming:** Use unique file names for each batch to avoid duplicates.
- **Checkpoints:** Use a unique checkpoint directory for each streaming query.
- **Performance:** For production, consider tuning options like `cloudFiles.maxFilesPerTrigger`.