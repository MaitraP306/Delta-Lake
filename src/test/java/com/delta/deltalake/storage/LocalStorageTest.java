package com.delta.deltalake.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LocalStorageTest {
    @Test
    void shouldWriteReadCreateListAndDelete() throws Exception {
        Storage storage = new LocalStorage(Files.createTempDirectory("delta-storage"));
        byte[] data = "hello".getBytes();
        storage.write("a/b.txt", data);
        assertArrayEquals(data, storage.read("a/b.txt"));
        assertTrue(storage.exists("a/b.txt"));
        assertEquals(java.util.List.of("a/b.txt"), storage.list("a"));
        storage.delete("a/b.txt");
        assertFalse(storage.exists("a/b.txt"));
    }

    @Test
    void createMustBeExclusive() throws Exception {
        Storage storage = new LocalStorage(Files.createTempDirectory("delta-storage"));
        assertTrue(storage.create("x", new byte[]{1}));
        assertFalse(storage.create("x", new byte[]{2}));
        assertArrayEquals(new byte[]{1}, storage.read("x"));
    }

    @Test
    void listAfterReturnsOnlyLexicographicallyLaterKeys() throws Exception {
        Path root = Files.createTempDirectory("storage-list-after");
        Storage storage = new LocalStorage(root);
        storage.create("_delta_log/00000000000000000000.json", new byte[0]);
        storage.create("_delta_log/00000000000000000001.json", new byte[0]);
        storage.create("_delta_log/00000000000000000002.json", new byte[0]);
        storage.create("_delta_log/00000000000000000002.json", new byte[0]);
        storage.create("_delta_log/00000000000000000003.json", new byte[0]);
        List<String> result = storage.listAfter("_delta_log", "_delta_log/00000000000000000001.json");
        assertEquals(List.of("_delta_log/00000000000000000002.json", "_delta_log/00000000000000000003.json"), result);
    }


    @Test
    void listAfterReturnsOnlyLaterKeys() throws Exception {
        Path root = Files.createTempDirectory("storage-list-after");
        Storage storage = new LocalStorage(root);
        storage.create("_delta_log/00000000000000000000.json", new byte[0]);
        storage.create("_delta_log/00000000000000000001.json", new byte[0]);
        storage.create("_delta_log/00000000000000000002.json", new byte[0]);
        List<String> result = storage.listAfter("_delta_log", "_delta_log/00000000000000000000.json");
        assertEquals(List.of("_delta_log/00000000000000000001.json", "_delta_log/00000000000000000002.json"), result);
    }
}
