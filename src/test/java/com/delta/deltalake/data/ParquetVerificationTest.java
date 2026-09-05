package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ParquetVerificationTest {

    private TableSchema schema() {
        return TableSchema.fromJson("""
            {
              "type": "record",
              "name": "TestRecord",
              "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
            """);
    }

    private Row row(TableSchema schema, long id, String name, int age) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("age", age);
        return Row.of(schema, values);
    }

    @Test
    void writeAndReadParquetRoundTrip() throws Exception {
        TableSchema schema = schema();
        List<Row> rows = List.of(row(schema, 1L, "Alice", 25), row(schema, 2L, "Bob", 30), row(schema, 3L, "Charlie", 35));
        List<GenericRecord> records = rows.stream().map(r -> RowCodec.encode(r, schema)).toList();
        Path parquet = Files.createTempFile("delta-parquet-verification", ".parquet");
        ParquetWriter.write(parquet, schema.avroSchema(), records);
        assertTrue(Files.exists(parquet));
        assertTrue(Files.size(parquet) > 0);
        List<GenericRecord> readRecords = ParquetReader.read(parquet);
        assertEquals(3, readRecords.size());
        List<Row> decodedRows = readRecords.stream().map(RowCodec::decode).toList();
        assertEquals(1L, decodedRows.get(0).get("id"));
        assertEquals("Alice", decodedRows.get(0).get("name"));
        assertEquals(25, decodedRows.get(0).get("age"));
        assertEquals(2L, decodedRows.get(1).get("id"));
        assertEquals("Bob", decodedRows.get(1).get("name"));
        assertEquals(30, decodedRows.get(1).get("age"));
        assertEquals(3L, decodedRows.get(2).get("id"));
        assertEquals("Charlie", decodedRows.get(2).get("name"));
        assertEquals(35, decodedRows.get(2).get("age"));
    }

    @Test
    void parquetPreservesMultipleRows() throws Exception {
        TableSchema schema = schema();
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Row row = row(schema, i, "user-" + i, 20 + (i % 50));
            records.add(RowCodec.encode(row, schema));
        }
        Path parquet = Files.createTempFile("delta-parquet-many-rows", ".parquet");
        ParquetWriter.write(parquet, schema.avroSchema(), records);
        List<GenericRecord> readRecords = ParquetReader.read(parquet);
        assertEquals(100, readRecords.size());
        for (int i = 0; i < 100; i++) {
            Row decoded = RowCodec.decode(readRecords.get(i));
            assertEquals((long) i, decoded.get("id"));
            assertEquals("user-" + i, decoded.get("name"));
            assertEquals(20 + (i % 50), decoded.get("age"));
        }
    }
}