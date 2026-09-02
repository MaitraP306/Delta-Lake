package com.delta.deltalake;

import com.delta.deltalake.data.Record;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.table.DeltaTable;

import java.nio.file.Path;
import java.util.List;

public final class Main {
    private Main() {}
    public static void main(String[] args) throws Exception {
        Path root = args.length == 0 ? Path.of("data/demo-table") : Path.of(args[0]);
        DeltaTable table = DeltaTable.open(new LocalStorage(root));
        table.append(List.of(new Record(1, "Alice", 25), new Record(2, "Bob", 31), new Record(3, "Charlie", 28)));
        System.out.println("version=" + table.version());
        System.out.println("rows=" + table.readAll());
        System.out.println("history=" + table.history());
    }
}
