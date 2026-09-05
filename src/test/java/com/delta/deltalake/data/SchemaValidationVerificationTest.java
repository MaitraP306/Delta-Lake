package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaValidationVerificationTest {

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

    private Row validRow() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 100L);
        values.put("name", "Alice");
        values.put("age", 25);
        return Row.of(schema(), values);
    }

    @Test
    void validRowPassesSchemaValidation() {
        TableSchema schema = schema();
        Row row = validRow();
        GenericRecord record = RowCodec.encode(row, schema);
        assertDoesNotThrow(() -> SchemaValidator.validate(schema, record));
    }

    @Test
    void wrongTypeIsRejected() {
        TableSchema schema = schema();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "not-a-long");
        values.put("name", "Alice");
        values.put("age", 25);
        Row row = Row.of(schema, values);
        assertThrows(IllegalArgumentException.class, () -> RowCodec.encode(row, schema));
    }

    @Test
    void missingRequiredFieldIsRejected() {
        TableSchema schema = schema();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 100L);
        values.put("name", "Alice");
        Row row = Row.of(schema, values);
        assertThrows(IllegalArgumentException.class, () -> RowCodec.encode(row, schema));
    }

    @Test
    void unexpectedFieldIsRejected() {
        TableSchema schema = schema();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 100L);
        values.put("name", "Alice");
        values.put("age", 25);
        values.put("extra", "unexpected");
        assertThrows(IllegalArgumentException.class, () -> Row.of(schema, values));
    }
}