package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
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

    @Test
    void roundTripsWiderParquetTypesThroughGenericRows() throws Exception {
        TableSchema schema = TableSchema.fromJson("""
                {
                  "type":"record", "name":"RichRow",
                  "fields":[
                    {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
                    {"name":"day","type":{"type":"int","logicalType":"date"}},
                    {"name":"eventTime","type":{"type":"long","logicalType":"timestamp-millis"}},
                    {"name":"payload","type":"bytes"},
                    {"name":"tags","type":{"type":"array","items":"string"}},
                    {"name":"scores","type":{"type":"map","values":"int"}},
                    {"name":"details","type":{"type":"record","name":"Details","fields":[{"name":"ok","type":"boolean"},{"name":"label","type":["null","string"],"default":null}]}}
                  ]
                }
                """);

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("ok", true);
        details.put("label", "sample");
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("amount", new BigDecimal("1234.56"));
        values.put("day", LocalDate.of(2026, 9, 2));
        values.put("eventTime", Instant.parse("2026-09-02T12:34:56Z"));
        values.put("payload", ByteBuffer.wrap(new byte[]{1, 2, 3}));
        values.put("tags", List.of("a", "b"));
        values.put("scores", Map.of("x", 7));
        values.put("details", details);

        Row input = Row.of(schema, values);
        Path file = Files.createTempDirectory("parquet-rich").resolve("rich.parquet");
        ParquetWriter.write(file, schema.avroSchema(), List.of(RowCodec.encode(input, schema)));

        Row actual = RowCodec.decode(ParquetReader.read(file).getFirst());
        assertEquals(new BigDecimal("1234.56"), actual.get("amount"));
        assertEquals(LocalDate.of(2026, 9, 2), actual.get("day"));
        assertEquals(Instant.parse("2026-09-02T12:34:56Z"), actual.get("eventTime"));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), actual.get("payload"));
        assertEquals(List.of("a", "b"), actual.get("tags"));
        assertEquals(Map.of("x", 7), actual.get("scores"));
        assertEquals(Map.of("ok", true, "label", "sample"), actual.get("details"));
    }

}
