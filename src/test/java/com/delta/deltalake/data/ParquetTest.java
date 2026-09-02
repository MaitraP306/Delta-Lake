package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetTest {

    @Test
    void roundTripsRows() throws Exception {
        Path file = Files.createTempDirectory("parquet").resolve("data.parquet");
        List<Record> input = List.of(new Record(1, "Alice", 25), new Record(2, "Bob", 31));
        List<GenericRecord> encoded = input.stream().map(RecordCodec::encode).toList();
        ParquetWriter.write(file, RecordSchema.schema(), encoded);
        List<Record> actual = ParquetReader.read(file).stream().map(RecordCodec::decode).toList();
        assertEquals(input, actual);
    }
}