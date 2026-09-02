package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaValidatorTest {

    private static final TableSchema SCHEMA =new TableSchema(
                    new Schema.Parser().parse("""
                            {
                              "type": "record",
                              "name": "Person",
                              "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "name", "type": "string"},
                                {"name": "age", "type": "int"}
                              ]
                            }
                            """)
    );

    @Test
    void acceptsValidRecord() {
        GenericRecord record = new GenericData.Record(SCHEMA.avroSchema());
        record.put("id", 1L);
        record.put("name", "Alice");
        record.put("age", 25);

        assertDoesNotThrow(() -> SchemaValidator.validate(SCHEMA, record));
    }

    @Test
    void rejectsWrongLongType() {
        GenericRecord record = new GenericData.Record(SCHEMA.avroSchema());
        record.put("id", "not-a-long");
        record.put("name", "Alice");
        record.put("age", 25);

        assertThrows(IllegalArgumentException.class, () -> SchemaValidator.validate(SCHEMA, record));
    }

    @Test
    void rejectsMissingRequiredField() {
        GenericRecord record = new GenericData.Record(SCHEMA.avroSchema());
        record.put("id", 1L);
        record.put("name", "Alice");

        assertThrows(IllegalArgumentException.class, () -> SchemaValidator.validate(SCHEMA, record));
    }
}