# Databricks DLT with Autoloader Module

## Introduction

Delta Live Tables (DLT) and Autoloader can be combined to build robust, streaming data pipelines that automatically detect and ingest new data files as they land in cloud storage. This pattern is ideal for continuous ingestion scenarios, ensuring that new data is always processed without manual intervention.

## Why Combine DLT with Autoloader?

- **Autoloader** provides scalable, efficient file discovery and incremental ingestion.
- **DLT** adds declarative pipeline orchestration, monitoring, data quality, and easy transformation management.
- **Together:** You get end-to-end automation, reliability, and easy-to-maintain pipelines.

## Example: DLT Pipeline Using Autoloader (CSV Files)

Let’s build a pipeline to ingest sales data as soon as new files arrive in `/FileStore/data/input/`.

### 1. DLT Notebook Setup

```python
import dlt
from pyspark.sql.functions import *
```

### 2. Ingest Sales Data Using Autoloader

```python
@dlt.table(
    name="raw_sales",
    comment="Ingested sales data using Autoloader as streaming source."
)
def ingest_sales():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("inferSchema", True)
            .load("/FileStore/data/input/")
    )
```

**Explanation:**  
This creates a streaming DLT table (`raw_sales`) that will auto-update as new files are dropped in the input folder. The `cloudFiles` source activates Autoloader.

### 3. Ingest Static Reference Data (Customers, Products)


```python
@dlt.table(name="raw_customers")
def load_customers():
    return (
        spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load("/FileStore/data/input/customer_details.csv")
    )

@dlt.table(name="raw_products")
def load_products():
    return (
        spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load("/FileStore/data/input/product_catalog.csv")
    )
```

*For reference data, use batch reads (static tables) unless you expect those files to change frequently.*

### 4. Transform and Join Data

```python
@dlt.table(name="enriched_sales")
def transform_sales():
    sales = dlt.read_stream("raw_sales")
    customers = dlt.read("raw_customers").select(
        col("CustomerName").alias("CustomerName_cust"),
        col("CustomerID").alias("CustomerID_cust"),
        col("Address").alias("Address_cust"),
        col("EmailAddress").alias("EmailAddress_cust"),
        col("Country").alias("Country_cust"),
        col("PostalCode").alias("PostalCode_cust"),
        col("PhoneNumber").alias("PhoneNumber_cust"),
        col("City").alias("City_cust"),
        col("State").alias("State_cust")
    )
    products = dlt.read("raw_products").select(
        col("Item").alias("Item_prod"),
        col("Category").alias("Category_prod"),
        col("UnitPrice").alias("UnitPrice_prod"),
        col("ProductID").alias("ProductID_prod")
    )
    return (
        sales.join(customers, sales.CustomerName == customers.CustomerName_cust, "left")
             .join(products, sales.Item == products.Item_prod, "left")
    )
```

*Use `dlt.read_stream("raw_sales")` since it is a streaming source.*

### 5. Add Data Quality Checks (Expectations)

```python
@dlt.expect("valid_quantity", "Quantity > 0")
@dlt.expect("valid_price", "UnitPrice > 0")
@dlt.table(name="validated_sales")
def validate_sales():
    return dlt.read("enriched_sales")
```

*Enforces rules: only rows with positive `Quantity` and `UnitPrice` are kept.*

### 6. Query the Results
After running the pipeline, query your final validated table:

```python
# To display the validated_sales table, use the following within the DLT pipeline
@dlt.view(name="validated_sales_view")
def display_validated_sales():
    return dlt.read("validated_sales")
```
```

## Key Points & Best Practices

- Use **Autoloader** for scalable, reliable streaming ingestion.
- **DLT** handles orchestration, dependencies, and data quality.
- Reference data: Use batch reads unless it updates frequently—then you can use streaming here, too.
- Use `dlt.read_stream()` when downstream table expects streaming input.
- Monitor the pipeline using the DLT UI for lineage and quality metrics.
