package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataLayerVerificationTest {

    @Test
    void recordCodecRoundTripWorks() {
        Record original = new Record(42L, "Alice", 25);
        GenericRecord encoded = RecordCodec.encode(original);
        Record decoded = RecordCodec.decode(encoded);
        assertEquals(original, decoded);
    }

    @Test
    void recordCodecConvertsLegacyRecordToGenericRow() {
        Record record = new Record(42L, "Alice", 25);
        Row row = RecordCodec.toRow(record);
        assertEquals(42L, row.get("id"));
        assertEquals("Alice", row.get("name"));
        assertEquals(25, row.get("age"));
        assertEquals(List.of("id", "name", "age"), row.schema().fieldNames());
    }

    @Test
    void rowCodecRoundTripWorks() {
        TableSchema schema = TableSchema.fromJson("""
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
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 100L);
        values.put("name", "Bob");
        values.put("age", 30);
        Row original = Row.of(schema, values);
        GenericRecord encoded = RowCodec.encode(original, schema);
        Row decoded = RowCodec.decode(encoded);
        assertEquals(100L, decoded.get("id"));
        assertEquals("Bob", decoded.get("name"));
        assertEquals(30, decoded.get("age"));
    }

    @Test
    void tableSchemaExposesExpectedFields() {
        TableSchema schema = TableSchema.fromJson("""
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

        assertEquals(List.of("id", "name", "age"), schema.fieldNames());
        assertNotNull(schema.field("id"));
        assertNotNull(schema.field("name"));
        assertNotNull(schema.field("age"));
        assertNull(schema.field("does_not_exist"));
    }
}