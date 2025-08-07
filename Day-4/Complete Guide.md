# Table of Contents
1. [Introduction to Delta Lake](#introduction-to-delta-lake)
2. [Delta Lake Architecture](#delta-lake-architecture)
3. [Delta Lake vs Traditional Data Lakes](#delta-lake-vs-traditional-data-lakes)
4. [Creating and Managing Delta Tables](#creating-and-managing-delta-tables)
    - 4.1 [Creating Delta Tables](#creating-delta-tables)
    - 4.2 [Loading Data into Delta Tables](#loading-data-into-delta-tables)
    - 4.3 [Delta Table Properties and Configuration](#delta-table-properties-and-configuration)
5. [Basic Data Transformations](#basic-data-transformations)
    - 5.1 [DataFrame Transformations](#dataframe-transformations)
    - 5.2 [SQL Transformations](#sql-transformations)
6. [Data Cleaning Techniques](#data-cleaning-techniques)
    - 6.1 [Handling Missing Values](#handling-missing-values)
    - 6.2 [Removing Duplicates](#removing-duplicates)
    - 6.3 [Data Type Corrections](#data-type-corrections)
7. [End-of-Day ETL Pipeline Task](#end-of-day-etl-pipeline-task)
8. [Complex Transformations](#complex-transformations)
    - 8.1 [Joining Datasets](#joining-datasets)
    - 8.2 [Aggregations and Grouping](#aggregations-and-grouping)
    - 8.3 [Window Functions](#window-functions)
9. [SQL UDFs and JSON Data Handling](#sql-udfs-and-json-data-handling)
    - 9.1 [Introduction to SQL UDFs](#introduction-to-sql-udfs)
    - 9.2 [Creating and Using UDFs](#creating-and-using-udfs)
    - 9.3 [Handling JSON Data](#handling-json-data)
10. [Advanced Delta Lake Features](#advanced-delta-lake-features)
    - 10.1 [Schema Enforcement and Evolution](#schema-enforcement-and-evolution)
    - 10.2 [Time Travel and Versioning](#time-travel-and-versioning)
    - 10.3 [Optimization Techniques](#optimization-techniques)
11. [ETL Overview in Delta Lake](#etl-overview-in-delta-lake)

---

## 1. Introduction to Delta Lake
Delta Lake is an open-source storage layer that brings reliability, performance, and ACID (Atomicity, Consistency, Isolation, Durability) transactions to Apache Spark™ and big data workloads. It sits on top of your existing data lake (such as S3, ADLS, or GCS) and is fully compatible with Spark APIs.

**Key Features:**
- **ACID Transactions:** Ensures data integrity with serializable transactions.
- **Scalable Metadata Handling:** Leverages Spark's distributed processing to manage metadata for petabyte-scale tables.
- **Time Travel (Data Versioning):** Query previous versions of data for audits, rollbacks, or reproducibility.
- **Schema Enforcement:** Prevents bad data by enforcing predefined schemas.
- **Schema Evolution:** Allows schema changes to accommodate evolving data.
- **Unified Batch and Streaming:** Supports both batch and streaming data from a single source.
- **Upserts and Deletes:** Enables merge, update, and delete operations, simplifying complex data management tasks in traditional data lakes.

This enables data engineers to build reliable, performant, and scalable ETL pipelines on data lakes.

---

## 2. Delta Lake Architecture
Key components of Delta Lake architecture include:

- **Parquet Files:** Data stored in optimized columnar Parquet format.
- **Transaction Log (`_delta_log`):** Maintains an ordered sequence of JSON files capturing all changes to the table, enabling ACID guarantees.
- **Metadata Layer:** Tracks table schema, statistics, and version history.
- **Data Access Layer:** Supports scalable reads and writes with snapshot isolation.

**Diagram (conceptual):**
```
User Queries --> Delta Table (Parquet Files + _delta_log)
```

---

## 3. Delta Lake vs Traditional Data Lakes

| Feature                | Traditional Data Lake (e.g., Parquet on S3)         | Delta Lake                                      |
|------------------------|-----------------------------------------------------|-------------------------------------------------|
| ACID Transactions      | No (atomic at file level, not table)                | Yes                                             |
| Schema Enforcement     | No (can be inconsistent)                            | Yes (prevents data corruption)                  |
| Schema Evolution       | Manual and complex                                  | Yes (allows schema changes over time)           |
| Time Travel            | No (difficult to track versions)                    | Yes (built-in data versioning)                  |
| Upserts/Deletes        | Very difficult (requires rewriting data)            | Yes (MERGE, UPDATE, DELETE operations)          |
| Data Quality           | Often suffers from "data swamp" issues              | Improved due to transactions and schema features|
| Performance            | Can be suboptimal without indexing/partitioning     | Optimized with features like Z-Ordering, compaction |


---

## 4. Creating and Managing Delta Tables

### 4.1 Creating Delta Tables

You can create Delta tables using DataFrame API or SQL.

**Using DataFrame API:**
```python
# Sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)

# Write DataFrame as a Delta table
df.write.format("delta").mode("overwrite").save("/mnt/delta/my_delta_table")

# You can also register it as a table in the metastore
df.write.format("delta").mode("overwrite").saveAsTable("my_managed_delta_table")
```

# Register Delta table in Metastore for SQL access
You can also create Delta tables directly using SQL.

**Managed Table:** (Data and metadata managed by Databricks)
```sql
CREATE TABLE my_sql_delta_table (
    id INT,
    name STRING,
    country STRING
)
USING DELTA;
```

**Unmanaged (External) Table:** (Metadata managed by Databricks, data in a specified external location)
```sql
CREATE TABLE my_external_delta_table (
    id INT,
    name STRING
)
USING DELTA
LOCATION '/mnt/delta/my_external_data_location';
```

**Converting Existing Parquet Tables:**

*Python:*
```python
from delta.tables import *

# Convert a Parquet table at a given path
deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/mnt/parquet/my_existing_table`")
```

*SQL:*
```sql
-- Convert a Parquet table (if registered in metastore)
CONVERT TO DELTA database_name.parquet_table_name;

-- Convert Parquet files at a specific path
CONVERT TO DELTA parquet.`/mnt/delta/parquet_files_to_convert/`;
```
> **Note:** After conversion, the original Parquet files are not modified. Delta Lake creates a transaction log for the existing files.


### 4.2 Loading Data into Delta Tables

You can load data into Delta tables using DataFrame write operations or SQL `INSERT` statements.

#### a) Using DataFrame `save` or `insertInto` (PySpark):

```python
# Sample new data
newData = [("David", 4), ("Eve", 5)]
newDf = spark.createDataFrame(newData, ["name", "id"])

# Append data to an existing Delta table (by path)
newDf.write.format("delta").mode("append").save("/mnt/delta/my_delta_table")

# Or, if registered as a table in the metastore:
newDf.write.mode("append").insertInto("my_managed_delta_table")

# Overwrite existing data in the table (uncomment to use)
# newDf.write.format("delta").mode("overwrite").save("/mnt/delta/my_delta_table")
```

#### b) Using SQL `INSERT INTO`:

```sql
INSERT INTO my_sql_delta_table (id, name, country) VALUES (100, 'John Doe', 'USA');

INSERT INTO my_sql_delta_table
VALUES
    (101, 'Jane Smith', 'Canada'),
    (102, 'Peter Jones', 'UK');
```

### 4.3 Delta Table Properties and Configuration

- **Overwrite mode:** replaces existing data.
- **Append mode:** adds new data without replacing.
- **MergeSchema option:** enables schema evolution on append.

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/delta/table")
```

---

## 5. Basic Data Transformations

### 5.1 DataFrame Transformations

Common operations include:

| Operation        | Code Example                       |
|------------------|------------------------------------|
| Select columns   | `df.select("id", "name")`          |
| Filter rows      | `df.filter(df.id > 1)`             |
| Add new column   | `df.withColumn("age", lit(25))`    |
| Rename column    | `df.withColumnRenamed("name", "fullname")` |
| Drop column      | `df.drop("age")`                   |

```python
from pyspark.sql.functions import lit

df = df.withColumn("age", lit(30))
df = df.withColumnRenamed("name", "fullname")
df.select("id", "fullname", "age").show()
```

### 5.2 SQL Transformations

Query Delta tables with SQL:
```sql
SELECT id, name FROM people WHERE id > 1;
```

---

## 6. Data Cleaning Techniques

### 6.1 Handling Missing Values

Drop rows with missing data:
```python
df_clean = df.na.drop()
```

Fill missing values with defaults:
```python
df_filled = df.na.fill({"age": 0, "name": "Unknown"})
```

### 6.2 Removing Duplicates

Remove duplicate rows based on specified columns:
```python
df_unique = df.dropDuplicates(["id"])
```

### 6.3 Data Type Corrections

Cast columns to correct data types to avoid downstream errors:
```python
from pyspark.sql.functions import col

df = df.withColumn("id", col("id").cast("int"))
df = df.withColumn("salary", col("salary").cast("double"))
```

---

## 7. End-of-Day ETL Pipeline Task

**Goal:** Build an ETL pipeline that:
- Extracts raw data from CSV or JSON
- Cleans missing/duplicate data, corrects types
- Loads clean data into a Delta Table
- Verifies data correctness with basic queries

---

## 8. Complex Transformations

### 8.1 Joining Datasets

Example of inner join:
```python
joined_df = df1.join(df2, on="id", how="inner")
joined_df.show()
```
Supported join types: inner, left, right, full, semi, anti.

### 8.2 Aggregations and Grouping

Summarize data grouped by a key:
```python
df.groupBy("department").count().show()
df.groupBy("department").agg({"salary": "avg"}).show()
```

### 8.3 Window Functions

Perform calculations across partitions of data:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(windowSpec)).show()
```

---

## 9. SQL UDFs and JSON Data Handling

### 9.1 Introduction to SQL UDFs

User Defined Functions extend SQL by adding custom logic not available out-of-the-box.

### 9.2 Creating and Using UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def capitalize_name(name):
    return name.capitalize() if name else None

capitalize_udf = udf(capitalize_name, StringType())

df = df.withColumn("name_capitalized", capitalize_udf("name"))
df.show()
```

### 9.3 Handling JSON Data

Parse JSON strings into columns:
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

json_schema = StructType([
    StructField("city", StringType()),
    StructField("state", StringType())
])

df_json = spark.read.json("/path/to/jsonfile.json")

df_parsed = df_json.select(from_json(col("json_column"), json_schema).alias("parsed_json"))
df_parsed.select("parsed_json.city", "parsed_json.state").show()
```

---

## 10. Advanced Delta Lake Features

### 10.1 Schema Enforcement and Evolution

Delta Lake enforces schema during writes and supports schema evolution when enabled.
```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/delta/table")
```

### 10.2 Time Travel and Versioning

Query previous snapshots using version number or timestamp:
```sql
SELECT * FROM people VERSION AS OF 2;
SELECT * FROM people TIMESTAMP AS OF '2023-05-01 12:00:00';
```

### 10.3 Optimization Techniques

- **Z-Ordering:** Improves data skipping and read performance on specified columns.
    ```python
    spark.sql("OPTIMIZE people ZORDER BY (id)")
    ```
- **Compaction (Vacuum):** Removes old files to reduce small file overhead.
    ```python
    spark.sql("VACUUM people RETAIN 168 HOURS")
    ```

---

## 11. ETL Overview in Delta Lake

ETL with Delta Lake consists of:

- **Extract:** Load raw data from various sources (CSV, JSON, Kafka).
- **Transform:** Clean data, apply business logic, join datasets, aggregate.
- **Load:** Write transformed data as Delta Tables, enabling ACID guarantees and performance optimizations.

Delta Lake unifies batch and streaming ETL and supports incremental data updates, making pipelines reliable and efficient.