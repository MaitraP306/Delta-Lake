package com.delta.deltalake.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LocalStorageTest {

    @Test
    void shouldWriteAndReadObject() throws Exception {
        Path root = Files.createTempDirectory("delta-test");
        Storage storage = new LocalStorage(root);

        byte[] data = "hello".getBytes();

        storage.write("test/file.txt", data);

        assertTrue(storage.exists("test/file.txt"));
        assertArrayEquals(data, storage.read("test/file.txt"));
    }

    @Test
    void shouldCreateObjectOnlyOnce() throws Exception {
        Path root = Files.createTempDirectory("delta-test");
        Storage storage = new LocalStorage(root);

        byte[] first = "first".getBytes();
        byte[] second = "second".getBytes();

        assertTrue(storage.create("test/object", first));
        assertFalse(storage.create("test/object", second));

        assertArrayEquals(first, storage.read("test/object"));
    }
}