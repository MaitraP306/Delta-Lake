# Delta Lake

A Java implementation of the core ideas behind **Delta Lake**: a transactional storage layer that brings ACID semantics, versioned table state, schema management, data skipping, optimization, and reliable object-store persistence to Parquet-based data.

This project is an implementation inspired by the paper:

> **Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores**  
> Armbrust et al., PVLDB 2020

The implementation is designed around a clean separation between the **table layer**, **transaction log**, **snapshot management**, **data layer**, and **storage layer**, making the system easy to understand, test, and extend.

---

## Architecture

At a high level, the project is organized as:

```text
                         DeltaTable
                              │
              ┌───────────────┼────────────────┐
              │               │                │
              ▼               ▼                ▼
       Snapshot Manager   Transaction Log   Data Layer
              │               │                │
              ▼               ▼                ▼
        Table State       JSON Actions       Parquet
              │
              ▼
        Storage Abstraction
              │
        ┌─────┴─────┐
        ▼           ▼
   LocalStorage   S3Storage
```

The main design principle is that **Delta metadata and table state are separated from the physical data files**.

### Transaction Log

Each table maintains a `_delta_log` containing versioned transaction files.

Transactions record actions such as:

- `AddFile`
- `RemoveFile`
- `Metadata`
- `Protocol`

The transaction log provides the durable history needed to reconstruct table state.

```text
_delta_log/
    00000000000000000000.json
    00000000000000000001.json
    00000000000000000002.json
    ...
```

### Snapshots

A snapshot represents the table state at a particular version.

The snapshot manager:

1. Locates the latest checkpoint when available.
2. Loads checkpoint state.
3. Reads newer transaction-log actions.
4. Applies additions/removals.
5. Produces the current table state.

This gives the table a consistent, versioned view of its data.

### ACID Transactions

Table mutations are committed through the transaction log rather than by modifying existing data files in place.

The implementation includes:

- atomic transaction commits,
- optimistic concurrency control,
- commit retries,
- cleanup of failed writes,
- exactly-once application transaction handling.

This allows multiple writers to safely operate on the same table.

### Checkpoints

Checkpoints periodically materialize table state so that opening a large table does not require replaying its complete transaction history.

```text
Checkpoint
    │
    ├── existing table state
    │
    ▼
new transaction-log versions
    │
    ▼
current Snapshot
```

The checkpoint layer includes Parquet-based checkpoint encoding and decoding.

### Data Layer

Table data is stored as Parquet files.

The data layer handles:

- schema-aware record encoding,
- Parquet writing,
- Parquet reading,
- file-level statistics,
- schema evolution,
- row and file operations.

The separation between the data layer and transaction layer means that the transaction log tracks **which data files belong to a table**, while Parquet stores the actual records.

### Statistics and Data Skipping

Parquet files maintain file-level statistics that can be used to avoid reading files that cannot contain matching records.

For example:

```text
Query:
sourceIP = X

       │
       ▼

File statistics
       │
   ┌───┴────┐
   │        │
cannot     may
match      match
   │        │
skip       read
```

This provides an important bridge between the logical table and the physical organization of its data.

### Z-Ordering

The table layer supports Z-order-based optimization for multidimensional data.

Instead of sorting exclusively by one column, Z-ordering interleaves bits from multiple dimensions to improve locality across several query columns.

```text
Column A ─┐
Column B ─┤
Column C ─┼──► Z-order key ──► sorted data files
Column D ─┘
```

This works together with file-level statistics to improve multidimensional data skipping.

### Table Optimization

The table layer includes optimization operations that rewrite existing data files into a more efficient physical layout.

These operations integrate with the transaction log so that rewritten files are represented as a new table version rather than silently modifying the current table state.

### Schema Evolution

Schemas are represented as table metadata and carried through transactions.

The implementation supports schema-aware operations including:

- schema creation,
- schema validation,
- compatible schema evolution,
- metadata updates,
- schema-aware Parquet encoding.

### Time Travel and Table History

Because table state is represented by versions in the transaction log, the table can be opened at historical versions.

This enables:

```text
Version 0 ──► Version 1 ──► Version 2 ──► Version 3
                  │
                  └── historical table state
```

The table API also exposes transaction history and version information.

### Storage Abstraction

The storage layer provides a common interface for table persistence.

The same Delta table implementation can operate against:

- local filesystem storage,
- Amazon S3.

This keeps storage-specific behavior isolated from the table and transaction logic.

For S3, the implementation uses the AWS SDK and supports object-store operations required by the Delta table.

---

## Project Structure

The main source tree is organized by responsibility:

```text
src/
├── main/
│   └── java/
│       └── com/delta/deltalake/
│           ├── data/
│           │   ├── checkpoint/
│           │   ├── parquet/
│           │   └── record/
│           │
│           ├── log/
│           │   ├── transaction log
│           │   ├── actions
│           │   └── version handling
│           │
│           ├── storage/
│           │   ├── Storage
│           │   ├── LocalStorage
│           │   └── S3Storage
│           │
│           ├── table/
│           │   ├── DeltaTable
│           │   ├── SnapshotManager
│           │   ├── CheckpointManager
│           │   └── table operations
│           │
│           └── experiments/
│               └── benchmark programs
│
└── test/
    └── java/
        └── com/delta/deltalake/
            └── ...
```

The exact package layout may evolve as the implementation grows, but the core separation remains:

```text
Table
  ↓
Snapshot / Transactions
  ↓
Data
  ↓
Storage
```

---

## Requirements

### Java

The project uses:

- **Java 21**
- Maven

Verify your Java installation:

```bash
java -version
```

Verify Maven:

```bash
mvn -version
```

### Amazon S3

S3 is optional for local development.

For S3-backed experiments you need:

- an AWS account,
- an S3 bucket,
- AWS credentials with access to that bucket,
- the AWS region used by the bucket.

The project uses the AWS SDK for Java.

---

## Build

Clone the repository:

```bash
git clone <your-repository-url>
cd Delta-Lake
```

Build the project:

```bash
mvn clean package
```

---

## Run the Test Suite

Run all tests:

```bash
mvn clean test
```

The test suite covers the major layers of the system, including:

- storage,
- Parquet/data handling,
- transaction logging,
- snapshots,
- table operations,
- checkpoints,
- schema evolution,
- exactly-once transactions,
- optimistic concurrency,
- S3 integration,
- optimization,
- Z-ordering,
- benchmark-specific functionality.

To run a specific test class:

```bash
mvn -Dtest=TestClassName test
```

For example:

```bash
mvn -Dtest=DeltaTableConcurrencyVerificationTest test
```

---

## Basic Java API

A Delta table can be opened through the `DeltaTable` API and used for normal table operations.

Conceptually:

```java
Storage storage = ...;

DeltaTable table = DeltaTable.open(storage);

table.append(rows);
table.delete(predicate);
table.upsert(rows);
table.merge(...);

List<Row> rows = table.readRows();
```

The same table abstraction can be backed by local storage or S3.

---

## Local Storage

Local storage is useful for development and fast iteration.

A typical table layout looks like:

```text
my-table/
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── ...
│
└── data/
    ├── part-....parquet
    ├── part-....parquet
    └── ...
```

The transaction log describes the logical table state, while the Parquet files contain the physical data.

---

## S3 Setup

Set the AWS profile you want the Java AWS SDK to use:

```bash
export AWS_PROFILE=delta-experiment
```

Set your bucket and region:

```bash
export S3_BUCKET=<your-bucket>
export AWS_REGION=<your-region>
```

Verify AWS credentials:

```bash
aws sts get-caller-identity --profile delta-experiment
```

The Java application will use the AWS credential provider chain to obtain credentials.

### S3-backed table

Benchmark commands can be configured with:

```text
--backend s3
--bucket <bucket>
--prefix <table-prefix>
--region <region>
```

---

## Experiments

Benchmark programs live under:

```text
src/main/java/com/delta/deltalake/experiments/
```

The benchmark entry point can be run with Maven:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.delta.deltalake.experiments.BenchmarkMain \
  -Dexec.args="<command> <options>"
```

### Metadata Discovery

The metadata benchmark compares object-store directory discovery with Delta snapshot reconstruction.

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.delta.deltalake.experiments.BenchmarkMain \
  -Dexec.args="metadata \
    --backend s3 \
    --bucket $S3_BUCKET \
    --prefix $S3_PREFIX \
    --region us-east-2 \
    --files 10000 \
    --rows-per-file 1000"
```

### Z-Ordering

The Z-order benchmark exercises multidimensional data organization and statistics-based file skipping.

Example:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.delta.deltalake.experiments.BenchmarkMain \
  -Dexec.args="z-order \
    --backend s3 \
    --bucket $S3_BUCKET \
    --prefix $S3_PREFIX \
    --region us-east-2 \
    --files 100 \
    --rows-per-file 10000 \
    --upload-threads 16"
```

---

## Development Workflow

A useful development cycle is:

```text
Make a change
     │
     ▼
mvn clean test
     │
     ▼
Run focused test
     │
     ▼
Run local experiment
     │
     ▼
Run S3 experiment when needed
     │
     ▼
Commit
```

For changes to the table or transaction layers, run the complete test suite before committing:

```bash
mvn clean test
```

---

## Design Principles

The implementation follows several principles:

### 1. Separation of concerns

Storage, transactions, snapshots, data encoding, and table operations are kept in separate layers.

### 2. Immutable data files

Table updates are represented through new data files and transaction-log actions rather than modifying existing table files in place.

### 3. Explicit table versions

Every successful transaction creates a new table version.

### 4. Transaction-log-driven state

The transaction log is the source of truth for the logical table state.

### 5. Optimistic concurrency

Concurrent writers coordinate through commit validation rather than requiring a global lock.

### 6. Storage independence

The table layer works through a storage abstraction so that local filesystems and object storage can share the same table implementation.

### 7. Test-driven validation

Core table semantics and concurrency behavior are covered by automated tests.

---

## Technology Stack

| Component | Technology |
|---|---|
| Language | Java 21 |
| Build | Maven |
| Data format | Apache Parquet |
| Serialization | Avro / JSON |
| Object storage | Amazon S3 |
| AWS client | AWS SDK for Java |
| Testing | JUnit 5 |
| Distributed integration | Apache Spark-compatible architecture |

---

## References

This project is based on the architecture described in:

**Armbrust et al. — "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores."**

The paper describes the design of Delta Lake around a transaction log layered over cloud object storage, providing reliable table state, ACID transactions, scalable metadata handling, and data-management optimizations.

---

## License

This repository is intended as a research and educational implementation of the ideas described in the referenced Delta Lake paper.

See the repository license for the terms applicable to this implementation.
