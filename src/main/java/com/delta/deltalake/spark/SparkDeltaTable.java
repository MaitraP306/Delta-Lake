
package com.delta.deltalake.spark;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.S3Storage;
import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.table.DeltaTable;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.util.*;

public final class SparkDeltaTable {
    private SparkDeltaTable() {}

    public static Dataset<org.apache.spark.sql.Row> read(SparkSession spark, DeltaTable table) throws IOException {
        return read(spark, table, table.version());
    }

    public static Dataset<org.apache.spark.sql.Row> read(SparkSession spark, DeltaTable table, long version) throws IOException {
        Objects.requireNonNull(spark);
        Objects.requireNonNull(table);
        var snapshot = table.snapshot(version);
        TableSchema schema = TableSchema.fromJson(snapshot.metadata().schemaString());
        List<com.delta.deltalake.log.AddFile> files = new ArrayList<>(snapshot.activeFiles());
        List<String> paths = new ArrayList<>();
        for (var file : files) paths.add(toSparkPath(table.storage(), file.path()));
        if (paths.isEmpty()) return spark.createDataFrame(Collections.emptyList(), SparkSchemaConverter.toSparkSchema(schema));
        Dataset<org.apache.spark.sql.Row> result = null;
        StructType targetSpark = SparkSchemaConverter.toSparkSchema(schema);
        for (int i = 0; i < files.size(); i++) {
            Dataset<org.apache.spark.sql.Row> physical = spark.read().parquet(paths.get(i));
            org.apache.spark.sql.Column[] projected = new org.apache.spark.sql.Column[targetSpark.fields().length];
            for (int j = 0; j < targetSpark.fields().length; j++) {
                StructField targetField = targetSpark.fields()[j];
                String sourceName = targetField.name();
                if (!hasField(physical.schema(), sourceName)) {
                    sourceName = null;
                    Schema.Field avroField = schema.avroSchema().getField(targetField.name());
                    for (String alias : avroField.aliases()) {
                        if (hasField(physical.schema(), alias)) {
                            sourceName = alias;
                            break;
                        }
                    }
                }
                if (sourceName == null) {
                    projected[j] = org.apache.spark.sql.functions.lit(null).cast(targetField.dataType()).alias(targetField.name());
                } else {
                    projected[j] = physical.col(sourceName).cast(targetField.dataType()).alias(targetField.name());
                }
            }
            Dataset<org.apache.spark.sql.Row> normalized = physical.select(projected);
            result = result == null ? normalized : result.unionByName(normalized);
        }
        return result;
    }

    public static long append(DeltaTable table, Dataset<org.apache.spark.sql.Row> dataset) throws IOException {
        Objects.requireNonNull(table);
        Objects.requireNonNull(dataset);
        TableSchema target = table.exists() ? table.currentSchema() : SparkSchemaConverter.toTableSchema(dataset.schema());
        List<Row> rows = dataset.collectAsList().stream().map(sparkRow -> toDeltaRow(sparkRow, dataset.schema(), target)).toList();
        if (rows.isEmpty()) throw new IllegalArgumentException("Spark dataset cannot be empty");
        return table.appendRows(rows);
    }

    public static void createOrReplaceTempView(SparkSession spark, DeltaTable table, String viewName) throws IOException {
        Objects.requireNonNull(viewName);
        if (viewName.isBlank()) throw new IllegalArgumentException("viewName cannot be blank");
        read(spark, table).createOrReplaceTempView(viewName);
    }

    public static String pathFor(Storage storage, String key) {
        return toSparkPath(storage, key);
    }

    private static String toSparkPath(Storage storage, String key) {
        if (storage instanceof LocalStorage local) return local.root().resolve(key).normalize().toUri().toString();
        if (storage instanceof S3Storage s3) {
            String prefix = s3.prefix();
            return "s3a://" + s3.bucket() + (prefix.isEmpty() ? "/" : "/" + prefix + "/") + key;
        }
        throw new IllegalArgumentException("Unsupported storage for Spark integration: " + storage.getClass().getName());
    }

    private static boolean hasField(StructType schema, String name) {
        for (String field : schema.fieldNames()) if (field.equals(name)) return true;
        return false;
    }

    private static Row toDeltaRow(org.apache.spark.sql.Row sparkRow, StructType sourceSchema, TableSchema targetSchema) {
        Map<String, Object> values = new LinkedHashMap<>();
        StructField[] fields = sourceSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            DataType type = fields[i].dataType();
            Object raw = sparkRow.isNullAt(i) ? null : sparkValue(sparkRow, i, type);
            values.put(fields[i].name(), raw);
        }
        return Row.of(targetSchema, values);
    }

    private static Object sparkValue(org.apache.spark.sql.Row row, int index, DataType type) {
        if (row.isNullAt(index)) return null;
        if (type instanceof ArrayType array) {
            List<?> input = row.getList(index);
            List<Object> result = new ArrayList<>(input.size());
            for (Object element : input) result.add(convertValue(element, array.elementType()));
            return result;
        }
        if (type instanceof MapType map) {
            Map<?, ?> input = row.getJavaMap(index);
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : input.entrySet()) {
                result.put(String.valueOf(entry.getKey()), convertValue(entry.getValue(), map.valueType()));
            }
            return result;
        }
        if (type instanceof StructType struct) {
            return convertStruct(row.getStruct(index), struct);
        }
        return convertValue(row.get(index), type);
    }

    private static Map<String, Object> convertStruct(org.apache.spark.sql.Row row, StructType schema) {
        Map<String, Object> result = new LinkedHashMap<>();
        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            result.put(fields[i].name(), sparkValue(row, i, fields[i].dataType()));
        }
        return result;
    }

    private static Object convertValue(Object value, DataType type) {
        if (value == null) return null;
        if (type instanceof DateType) {
            if (value instanceof LocalDate date) return date;
            if (value instanceof java.sql.Date date) return date.toLocalDate();
        }
        if (type instanceof TimestampType || type.typeName().equalsIgnoreCase("timestamp_ntz")) {
            if (value instanceof Instant instant) return instant;
            if (value instanceof java.sql.Timestamp timestamp) return timestamp.toInstant();
            if (value instanceof LocalDateTime local) return local.toInstant(ZoneOffset.UTC);
        }
        if (type instanceof BinaryType && value instanceof byte[] bytes) return bytes;
        if (type instanceof DecimalType && value instanceof java.math.BigDecimal decimal) return decimal;
        if (type instanceof ArrayType array && value instanceof scala.collection.Seq<?> seq) {
            List<Object> result = new ArrayList<>();
            scala.collection.Iterator<?> iterator = seq.iterator();
            while (iterator.hasNext()) result.add(convertValue(iterator.next(), array.elementType()));
            return result;
        }
        if (type instanceof MapType map && value instanceof scala.collection.Map<?, ?> scalaMap) {
            Map<String, Object> result = new LinkedHashMap<>();
            scala.collection.Iterator<?> iterator = scalaMap.iterator();
            while (iterator.hasNext()) {
                scala.Tuple2<?, ?> pair = (scala.Tuple2<?, ?>) iterator.next();
                result.put(String.valueOf(pair._1()), convertValue(pair._2(), map.valueType()));
            }
            return result;
        }
        if (type instanceof StructType struct && value instanceof org.apache.spark.sql.Row nested) {
            return convertStruct(nested, struct);
        }
        return value;
    }
}
