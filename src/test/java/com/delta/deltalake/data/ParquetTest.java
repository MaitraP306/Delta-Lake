package com.delta.deltalake.data;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetTest {

    @Test
    void shouldWriteAndReadParquet() throws Exception {
        Path directory = Files.createTempDirectory("parquet-test");
        Path file = directory.resolve("data.parquet");

        List<Record> input = List.of(
                new Record(1, "Alice", 25),
                new Record(2, "Bob", 31),
                new Record(3, "Charlie", 28)
        );

        ParquetWriter.write(file, input);

        List<Record> output = ParquetReader.read(file);

        assertEquals(input, output);
    }
}