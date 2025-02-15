# **Interview questions and answers**

## **Delta Lakehouse**
A Delta Lakehouse combines the best features of data lakes and data warehouses,
providing a unified platform that supports both structured and unstructured data while
enabling transactional consistency. It uses Delta Lake, an open-source storage layer built
on top of Apache Spark and Parquet, to enhance data lakes with features like ACID
transactions, scalable metadata handling, and data versioning.

### **Key Features of Delta Lakehouse:**
- **Unified Storage:** It allows both structured (tables, data warehouses) and
unstructured (raw data, JSON, logs) data in one place.
- **ACID Transactions:** Ensures data consistency and reliability even in large,
distributed environments.
- **Time Travel:** Supports querying historical versions of data.
- **Schema Enforcement:** Ensures data quality by automatically enforcing schema
checks when new data is ingested.
- **Scalability:** Handles large volumes of data and scales with cloud-based
infrastructure.

Delta Lakehouses provide the ability to perform advanced analytics, machine learning, and
real-time data processing within one system, making it ideal for modern data architectures.

## **Delta Table**
A Delta Table is a table stored in Delta Lake, an optimized data format built on Apache
Parquet, designed for large-scale data processing and analytics.

### **Key Features of Delta Tables:**
- **ACID Transactions:** Delta Tables support transactions (insert, update, delete),
ensuring data integrity and consistency.
- **Schema Evolution:** The table can automatically evolve its schema as new data is
added without manual intervention.
- **Time Travel:** You can query previous versions of a Delta Table using a feature called
time travel, allowing you to access historical data.
- **Efficient Updates:** Delta Tables allow for faster updates, upserts, and deletes
compared to traditional tables.
- **Partitioning:** Supports partitioning for optimized query performance, especially
with large datasets.

### **Importance of Delta Tables:**
- **Data Consistency:** Delta Tables ensure reliable and consistent data storage,
important for analytics and real-time applications.
- **Optimized Performance:** Delta optimizes the storage and query performance of
large datasets, ensuring faster analytics.
- **Data Integrity:** With features like ACID transactions, Delta Tables help prevent data
corruption, making them crucial for enterprise data architectures.
- **Historical Data Analysis:** With time travel, Delta Tables allow you to analyze
historical data, making it easier to perform audits or compare different data
snapshots over time.

In summary, Delta Tables are central to the Delta Lakehouse architecture, providing a
robust, scalable, and reliable foundation for modern data processing and analytics.

## **What is managed tables and Unmanaged tables?**
### **Key Differences:**
| Feature          | Managed Tables                              | Unmanaged Tables                         |
|------------------|---------------------------------------------|------------------------------------------|
| **Data Management** | System manages both metadata and data.     | Only metadata is managed; data is external. |
| **Data Deletion**   | Data is deleted when the table is dropped. | Data remains in the external location when dropped. |
| **Storage Location**| Data is stored within the system's default location. | Data can be stored anywhere (e.g., S3, HDFS). |

## **What is Cores and Partitions?**
Cores are processing units within a CPU that execute tasks, while partitions are chunks of
data that are distributed across cores or nodes for parallel processing.
The relation between them is that partitions allow data to be divided into smaller pieces,
which can then be processed in parallel across the available cores. More cores can process
more partitions simultaneously, leading to faster data processing in distributed systems like
Apache Spark.

## **How do you handle bad records?**
In Databricks, you can handle bad records with the following options:
1. **Permissive:** Default mode, processes good records and stores bad records with null
or error messages.
```python
df = spark.read.option("mode", "PERMISSIVE").csv("path")
```
2. **Drop Malformed:** Ignores and skips bad records.
```python
df = spark.read.option("mode", "DROPMALFORMED").csv("path")
```
3. **Fail Fast:** Stops processing and throws an error on encountering bad records.
```python
df = spark.read.option("mode", "FAILFAST").csv("path")
```
4. **Bad Record Path:** Stores bad records in a specified path for later review.
```python
df = spark.read.option("badRecordsPath", "/path/to/bad_records").csv("path")
```

## **Difference between narrow and wide transformations?**
Narrow transformations involve operations that can be done within a single partition
(no shuffle).
Examples: `map()`, `filter()`, `union()`, `sample()`
Wide transformations require data to be shuffled between partitions, which can be more
resource-intensive.
Examples: `groupBy()`, `reduceByKey()`, `join()`, `distinct()`

## **Difference between sort and order by?**
- **sort():** Sorts data within partitions before a shuffle or final output.
- **orderBy():** Sorts data after the shuffle, ensuring the final output is sorted across all
partitions.

## **Maximum size of worker node?**
The maximum size of a worker node depends on the cloud provider and instance type:
- **AWS:** Up to 96 vCPUs and 768 GB RAM (e.g., p4d.24xlarge).
- **Azure:** Up to 96 vCPUs and 672 GB RAM (e.g., Standard_ND96asr_v4).
- **GCP:** Up to 96 vCPUs and 624 GB RAM (e.g., n2d-highmem-96).

Worker nodes can scale horizontally or vertically based on your needs.

## **How to select worker/driver types in Databricks?**
In Databricks, you can select worker and driver types when creating a cluster:
1. **Automatic:** Default types are selected based on your cluster configuration.
2. **Manual:** Choose specific types under Worker Type and Driver Type when creating a
cluster.
   - For heavy workloads (e.g., ML), select high CPU and memory instances (e.g.,
p3, r5).
   - For light workloads, select smaller instances (e.g., m5, t3).

## **How to choose cluster config work types?**
To choose cluster configurations and worker types in Databricks:
1. **Heavy workloads (e.g., ML, data processing):** Choose high vCPUs and RAM
instances (e.g., r5, p3).
2. **General-purpose tasks (e.g., ETL, analytics):** Choose balanced instances (e.g., m5,
t3).
3. **Light workloads (e.g., small data, dev):** Choose smaller instances (e.g., t3.micro,
m5.large).

Select based on the cloud provider (AWS, Azure, GCP) and consider cost and resource
needs.

## **What is repartition and coalesce?**
Use `repartition()` for increasing partitions and `coalesce()` for decreasing partitions (avoids
a full shuffle).
```python
df = df.repartition(200) # For more parallelism
df = df.coalesce(50) # To reduce the number of partitions
```

## **PySpark optimization techniques?**
1. **Caching/Persisting:** Cache frequently used data to avoid recomputation.
2. **Broadcast Variables:** Broadcast small datasets to avoid shuffling.
3. **Repartition/Coalesce:** Control partitioning to optimize parallelism and reduce
shuffling.
4. **Avoid Shuffles:** Minimize expensive operations like `groupBy()` and `join()`.
5. **Column Pruning:** Select only necessary columns.
6. **Predicate Pushdown:** Apply filters early to reduce processed data.
7. **Tune Spark Configs:** Adjust settings like `spark.sql.shuffle.partitions`.
8. **Use DataFrame API:** Prefer over RDD for better optimization.
9. **Avoid `collect()` on large data:** Use `take()` for smaller samples.

## **What is Interactive and Job Cluster?**
- **Interactive Cluster:** Long-running, used for data exploration, development, and
running notebooks interactively. Created through Cluster UI.
- **Job Cluster:** Short-lived, created for specific jobs (e.g., batch processing), and
terminated after the job completes. Job API and Job UI.

## **What is Default partition size? Default number?**
- **Default Number of Partitions:** 200 partitions for shuffles.
- **Default Partition Size:** Typically around 128 MB per partition.

## **What is auto scaling in Databricks?**
Auto-scaling in Databricks automatically adjusts the number of worker nodes in a cluster
based on workload demand.
- **Scale Up:** Adds nodes during high demand.
- **Scale Down:** Removes nodes during low demand.

This helps optimize performance and reduce costs.

## **What is `map()` and `mapPartitions()`?**
- **map():** Applies a function to each element in the RDD/DataFrame.
- **mapPartitions():** Applies a function to each partition in the RDD/DataFrame,
improving performance for large datasets.

## **Broadcast variable**
Broadcast variables are read-only variables and can be copied on all workers. We cannot
change the value of these variables.
If the variable is not broadcasted then the variable will be available in only Driver.

### **Purpose:** 
Distribute large, read-only data to all worker nodes efficiently.

### **Modifiability:** 
Read-only; distributed to all nodes once.

### **Use Case:** 
Sharing large lookup tables or configuration data across tasks.

### **Limitation:** 
Cannot be modified after being broadcast; large data can consume
significant memory.

## **Accumulators:**
Accumulators are variables which are used to aggregate information from multiple
executors.

### **Purpose:** 
Used for aggregating values (e.g., sum or count) across distributed
tasks.

### **Modifiability:** 
Writable only by workers, readable by the driver.

### **Use Case:** 
Tracking totals, errors, or counts across tasks.

### **Limitation:** 
Not suitable for complex transformations; can only be read by the
driver.

### **Feature Comparison:**
| Feature          | Accumulators                              | Broadcast Variables                         |
|------------------|---------------------------------------------|------------------------------------------|
| **Purpose** | For aggregating values (e.g., sum, count) across nodes     | For efficiently distributing read-only data across nodes |
| **Modifiability**   | Writable only by workers, readable by driver only | Read-only for all tasks; distributed once to all workers |
| **Use Case**| Counting, summing, and tracking values | Distributing large lookup tables, configuration data |
| **Visibility**| Can only be read on the driver node | Can be read by all worker nodes during tasks |
| **Fault Tolerance**| Accumulators can be used for error tracking | Broadcasted data is efficiently replicated but not fault-tolerant |
| **Efficiency**| Useful for simple aggregations and counters | Useful for distributing large, immutable datasets efficiently |

## **Apache Spark Architecture:**
Spark works in a master-slave architecture where the master is called the “Driver”
and slaves are called “Workers”.

## **PySpark Architecture**
PySpark architecture consists of a driver program that coordinates tasks and
interacts with a cluster manager to allocate resources. The driver communicates
with worker nodes, where tasks are executed within an executor’s JVM.
SparkContext manages the execution environment, while the DataFrame API enables
high-level abstraction for data manipulation. SparkSession provides a unified entry
point for Spark functionality. Underneath, the cluster manager oversees resource
allocation and task scheduling across nodes, facilitating parallel computation for
processing large-scale data efficiently.

## **Spark Driver**
Spark Driver creates a context that is an entry point to your application, and all
operations (transformations and actions) are executed on worker nodes, and the
resources are managed by Cluster Manager.
- Spark Driver and SparkContext collectively watch over the spark job execution
within the cluster.
- Spark Driver contains various other components such as DAG Scheduler, Task
Scheduler, Backend Scheduler, and Block Manager, which are responsible for
translating the user-written code into jobs that are actually executed on the
cluster.

## **Cluster manager – (Resource allocation)**
- Driver requests cluster manager to allocation resources
- **Spark Stages:** Spark job is divided into Spark stages, where each stage
represents a set of tasks that can be executed in parallel.
- A stage consists of a set of tasks that are executed on a set of partitions of
the data.
- **DAG (Directed Acyclic Graph):** Spark schedules stages and tasks. Spark uses a
DAG (Directed Acyclic Graph) scheduler, which schedules stages of tasks. The
TaskScheduler is responsible for sending tasks to the cluster, running them,
retrying if there are failures, and mitigating stragglers.

## **Spark job:**
A job in Spark refers to a sequence of transformations on data.

- **Fault Tolerance:** Spark is fault-tolerant. If a task fails, it can be
automatically re-executed on another node in the cluster.

### **Transformations:**
- `flatMap()`, `map()`, `reduceByKey()`, `filter()`, `sortByKey()`

### **Actions:**
- `count()`, `collect()`, `first()`, `max()`, `reduce()`

## **What is Delta Lake?**
Delta Lake is the optimized storage layer that provides the foundation for tables in a
lakehouse on Databricks.

## **Delta tables:**
- Delta table we can use friendly names.
- Delta table we can use time travel.

## **Groupby and reduce by short answer difference?**
- **GroupBy:** Aggregates data by keys, and then you can apply operations on these
grouped data subsets. It’s typically used for operations like sum, average, count,
etc., on groups of data.
- **ReduceBy:** Combines data by keys in a more efficient manner by merging values
locally on each partition before sending data across the network. It’s generally
used to reduce the amount of data shuffled between nodes.

In summary, GroupBy groups data and then performs operations, while ReduceBy
reduces data first, making it more network-efficient.

## **What is a data plane?**
The term “data plane” refers to the area of the computer network that handles data
processing and storage.

## **What is a management plane?**
Databricks’ management plane is responsible for managing workspace operations,
security, monitoring, and cluster configuration.

## **ACID Properties:**
ACID stands for Atomicity, Consistency, Isolation, and Durability, which are key properties
of database transactions to ensure data integrity.

1. **Atomicity:** Ensures that all operations in a transaction are completed successfully,
or none at all.
   - Example: Transferring money between bank accounts, where both debit and
credit operations must succeed together.

2. **Consistency:** Ensures that a transaction brings the database from one valid state to
another.
   - Example: After a money transfer, the total amount in both accounts remains
the same.

3. **Isolation:** Ensures that concurrent transactions do not interfere with each other.
   - Example: Two users booking movie tickets at the same time will not book
the same seat.

4. **Durability:** Ensures that once a transaction is committed, it remains so even in the
event of a system failure.
   - Example: After booking a flight, the reservation remains confirmed even if
the system crashes right after.

These properties collectively ensure reliable transactions in database systems.

## **Difference between RDD and DataFrames:**
- **RDD (Resilient Distributed Dataset):** 
  - Fundamental data structure in Spark.
  - Distributed collection of objects that can be processed in parallel.
  - Immutable and can hold any type of object.
  - Low-level control over data and operations.
  - Methods like `map()`, `filter()`, and `reduce()`.
  - Lack optimizations like query planning and indexing.

- **DataFrames:**
  - Higher-level abstractions built on top of RDDs.
  - Similar to tables in a database.
  - Optimized, user-friendly interface (like SQL).
  - Rich transformations and actions like `select()`, `groupBy()`, `join()`.
  - Optimized through the Catalyst Optimizer.

## **Challenges in Spark:**
- **Memory Management:** In-memory computing can lead to memory-related challenges such as spilling data to disk when memory is insufficient, or out-of-memory errors during operations.
- **Data Skew:** Uneven data distribution across partitions can cause performance issues and delays.
- **Shuffling:** Shuffling data between nodes (especially during operations like `groupBy`, `join`, etc.) can be expensive and lead to increased execution time and resource usage.
- **Fault Tolerance:** While Spark provides fault tolerance via RDD lineage, large-scale failures can still pose issues in terms of recovery times and complexity.
- **Cluster Management:** Ensuring optimal resource allocation and managing cluster resources efficiently can be challenging when scaling Spark workloads.

## **reduceByKey and groupByKey:**
- **reduceByKey:** Merges the values for each key using an associative reduce function. It performs the reduce operation in a distributed manner, applying the reduction on each partition before the final shuffle, which reduces the amount of data shuffled.
- **groupByKey:** Collects all the values for each key and groups them together as a list (or iterable). While it's easier to understand, it can lead to inefficient shuffling and memory overhead as all values for a key are stored in memory before the shuffle.

## **Advantages of Parquet Files:**
- **Columnar Storage:** Highly efficient for querying large datasets. It only reads the necessary columns, reducing I/O operations.
- **Efficient Compression:** Due to its columnar nature, Parquet files can be heavily compressed, reducing storage space.
- **Schema Support:** Parquet files include the schema of the data, which makes reading and writing efficient and self-descriptive.
- **Optimized for Big Data:** Designed for large-scale data processing frameworks like Apache Spark and Hadoop.

## **Map and FlatMap:**
- **Map:** Applies a function to each element in the RDD or DataFrame and returns a new RDD or DataFrame. It maintains the one-to-one relationship between input and output.
- **FlatMap:** Similar to map, but it allows the transformation to produce multiple output elements for each input element, thus "flattening" the result. For example, flatMap can transform an element into a list, which is then flattened into individual elements.

## **Out of Memory:**
Out of memory (OOM) errors occur when Spark does not have enough memory to process the data being handled. This can be due to:
- Too many partitions or large datasets in a single partition.
- Inefficient operations causing memory pressure.
- Insufficient heap size or executor memory allocation.

Solutions include optimizing partitioning, adjusting memory settings (e.g., `spark.executor.memory`), or using disk storage for spilling.
`