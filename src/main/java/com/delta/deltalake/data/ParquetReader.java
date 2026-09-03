package com.delta.deltalake.data;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public final class ParquetReader {

    private ParquetReader() {}

    public static List<GenericRecord> read(java.nio.file.Path input) throws IOException {

        if (!Files.exists(input)) {
            throw new IOException("Parquet file does not exist: " + input);
        }

        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(input.toUri()), new Configuration());
        List<GenericRecord> records = new ArrayList<>();

        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).withDataModel(GenericData.get()).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }

        return records;
    }
}