package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public final class ParquetWriter {

    private static final Schema SCHEMA = new Schema.Parser().parse("""
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

    private ParquetWriter() {
    }

    public static void write(
            java.nio.file.Path output,
            List<Record> records
    ) throws IOException {

        Path hadoopPath = new Path(output.toUri());

        try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath).withSchema(SCHEMA).build()) {

            for (Record record : records) {
                GenericRecord value = new GenericData.Record(SCHEMA);

                value.put("id", record.id());
                value.put("name", record.name());
                value.put("age", record.age());

                writer.write(value);
            }
        }
    }
}