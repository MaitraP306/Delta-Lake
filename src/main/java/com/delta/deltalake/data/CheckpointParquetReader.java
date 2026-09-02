package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public final class CheckpointParquetReader {

    private CheckpointParquetReader() {}

    public static List<GenericRecord> read(java.nio.file.Path input) throws IOException {
        if (!Files.exists(input)) {
            throw new IOException("Checkpoint does not exist: " + input);
        }

        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(input.toUri()), new Configuration());
        List<GenericRecord> rows = new ArrayList<>();
        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) {
                rows.add(row);
            }
        }
        return rows;
    }
}