package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class RecordSchema {

    private static final Schema SCHEMA =new Schema.Parser().parse("""
                    {
                      "type": "record",
                      "name": "Record",
                      "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"}
                      ]
                    }
                    """);

    private RecordSchema() {}

    public static Schema schema() {
        return SCHEMA;
    }
    public static GenericRecord toGenericRecord(Record record) {
      GenericRecord value =new GenericData.Record(SCHEMA);

      value.put("id", record.id());
      value.put("name", record.name());
      value.put("age", record.age());

      return value;
    }
}