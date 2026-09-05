package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableParallelAppendVerificationTest {

    @Test
    void parallelAppendWritesOneFilePerGroupAndOneTransaction() throws Exception {
        Path root = Files.createTempDirectory("delta-parallel-append");
        LocalStorage storage = new LocalStorage(root);
        DeltaTable table = DeltaTable.open(
                storage,
                Integer.MAX_VALUE,
                List.of("bucket")
        );

        TableSchema schema =
                Row.infer(Map.of("id", 0L, "value", 0.0, "bucket", 0))
                        .schema();

        List<List<Row>> groups = new ArrayList<>();

        for (int file = 0; file < 8; file++) {
            List<Row> rows = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                long id = (long) file * 10 + i;

                Map<String, Object> values = new LinkedHashMap<>();
                values.put("id", id);
                values.put("value", (double) id);
                values.put("bucket", file);

                rows.add(Row.of(schema, values));
            }

            groups.add(rows);
        }

        long version = table.appendRowsParallel(groups, 4);

        assertEquals(0, version);
        assertEquals(80, table.readRows().size());
        assertEquals(8, table.snapshot().fileCount());
        assertEquals(8,
                storage.list("data").stream()
                        .filter(path -> path.endsWith(".parquet"))
                        .count());

        assertEquals(1, table.history().size());
        assertEquals(
                "WRITE",
                table.history().get(0).operation()
        );
    }

    @Test
    void parallelAppendRejectsInvalidThreadCount() throws Exception {
        DeltaTable table = DeltaTable.open(
                new LocalStorage(Files.createTempDirectory("delta-parallel-invalid"))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.appendRowsParallel(List.of(), 0)
        );
    }
}
