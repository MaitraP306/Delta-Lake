package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ParquetReader {

    private ParquetReader() {
    }

    public static List<Record> read(
            java.nio.file.Path input
    ) throws IOException {

        List<Record> records = new ArrayList<>();
        Configuration configuration = new Configuration();
        HadoopInputFile inputFile =HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(input.toUri()),configuration);


        try ( org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(inputFile)
                             .build()) {

            GenericRecord value;

            while ((value = reader.read()) != null) {
                records.add(
                        new Record(
                                (Long) value.get("id"),
                                value.get("name").toString(),
                                (Integer) value.get("age")
                        )
                );
            }
        }

        return records;
    }
}