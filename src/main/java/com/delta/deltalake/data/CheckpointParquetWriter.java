package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

public final class CheckpointParquetWriter {
  private CheckpointParquetWriter() {}
  public static void write(java.nio.file.Path output,Schema schema,Collection<? extends GenericRecord> rows) throws IOException {
    Files.deleteIfExists(output);
    HadoopOutputFile outputFile = HadoopOutputFile.fromPath(new Path(output.toUri()), new Configuration());
    try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(schema).build()) {
      for (GenericRecord row : rows) {
        writer.write(row);
      }
    }
  }
}