# Delta Lake

A Java 21 implementation inspired by *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores* (Armbrust et al., PVLDB 2020). The project explores how a transaction log can provide versioned, consistent table state on top of Parquet data files.

## Key Features

- **Transaction log and snapshots:** Versioned JSON actions reconstruct the current or historical table state.
- **Transactions:** Includes optimistic concurrency handling, commit retries, and failed-write cleanup.
- **Checkpoints:** Parquet-based checkpoints reduce the amount of transaction history needed to rebuild a snapshot.
- **Parquet data layer:** Reads and writes records, tracks file-level statistics, and supports schema-aware operations.
- **Query optimization:** File statistics enable data skipping; Z-ordering improves data locality for multi-column queries.
- **Storage abstraction:** Supports local storage and an S3 backend.
- **Table operations:** Includes append, delete, upsert, merge, history, and time-travel APIs.

## Architecture

The main layers are `DeltaTable`, snapshot and transaction management, the Parquet data layer, and a storage abstraction. The transaction log records which files belong to a table; Parquet files hold the actual records.

```text
DeltaTable
   ├── Transaction Log
   ├── Snapshot / Checkpoint Manager
   ├── Parquet Data Layer
   └── Storage (Local / S3)
```

## Requirements

- Java 21
- Maven
- AWS credentials and an S3 bucket only for S3-backed experiments

## Build and Test

```bash
mvn clean package
mvn clean test
```

Run a specific test class with:

```bash
mvn -Dtest=TestClassName test
```

## Experiments

Benchmark programs are under `src/main/java/com/delta/deltalake/experiments/`. They cover metadata discovery, data skipping, and Z-ordering. For example, run a local data-skipping experiment with generated data:

```bash
mvn -q exec:java \
  -Dexec.mainClass=com.delta.deltalake.experiments.BenchmarkMain \
  -Dexec.args="data-skipping --backend local --root benchmark-data --files 100 --rows-per-file 1000 --generate --warmups 3 --iterations 10"
```

S3-backed runs require a configured AWS profile, bucket, and region.

## Project Structure

```text
src/main/java/com/delta/deltalake/
├── data/         # Parquet, records, and checkpoints
├── log/          # Transaction actions and version handling
├── storage/      # Local and S3 storage
├── table/        # Table operations and snapshot management
└── experiments/  # Benchmarks
src/test/java/    # Tests
```

## Scope

This is a research and educational implementation of Delta Lake concepts, not the full Databricks Delta Lake product. Benchmark results depend on the data and environment; validate behavior and assumptions before drawing performance conclusions.

## Reference

Armbrust et al., *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores*, PVLDB 2020.

See the repository's `LICENSE` for licensing terms.
