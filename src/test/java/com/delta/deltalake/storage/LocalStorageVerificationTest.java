package com.delta.deltalake.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LocalStorageVerificationTest {

    @Test
    void verifyCompleteStorageContract() throws Exception {
        Path root = Files.createTempDirectory("delta-storage-verification");
        Storage storage = new LocalStorage(root);
        assertFalse(storage.exists("data/a.txt"));
        assertEquals(List.of(), storage.list("data"));
        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        storage.write("data/a.txt", hello);
        assertTrue(storage.exists("data/a.txt"));
        assertArrayEquals(hello, storage.read("data/a.txt"));
        assertEquals(5, storage.size("data/a.txt"));
        assertTrue(storage.modificationTimeMillis("data/a.txt") > 0);
        byte[] replacement = "replacement".getBytes(StandardCharsets.UTF_8);
        storage.write("data/a.txt", replacement);
        assertArrayEquals(replacement, storage.read("data/a.txt"));
        assertEquals(replacement.length, storage.size("data/a.txt"));
        assertTrue(storage.create("data/b.txt", new byte[]{1, 2, 3}));
        assertFalse(storage.create("data/b.txt", new byte[]{4, 5, 6}));
        assertArrayEquals(new byte[]{1, 2, 3}, storage.read("data/b.txt"));
        storage.write("data/nested/c.txt", "nested".getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of("data/a.txt", "data/b.txt", "data/nested/c.txt"), storage.list("data"));
        storage.create("_delta_log/00000000000000000000.json", new byte[0]);
        storage.create("_delta_log/00000000000000000001.json", new byte[0]);
        storage.create("_delta_log/00000000000000000002.json", new byte[0]);
        assertEquals(List.of("_delta_log/00000000000000000001.json", "_delta_log/00000000000000000002.json"), storage.listAfter("_delta_log", "_delta_log/00000000000000000000.json"));
        storage.delete("data/a.txt");
        assertFalse(storage.exists("data/a.txt"));
        assertEquals(List.of("data/b.txt", "data/nested/c.txt"), storage.list("data"));
        assertDoesNotThrow(() -> storage.delete("data/a.txt"));
    }

    @Test
    void verifyPathTraversalProtection() throws Exception {
        Storage storage = new LocalStorage(Files.createTempDirectory("delta-storage-security"));

        assertThrows(IllegalArgumentException.class, () -> storage.read("../outside.txt"));

        assertThrows(IllegalArgumentException.class, () -> storage.read("/absolute/path"));

        assertThrows(IllegalArgumentException.class, () -> storage.read("foo\\bar"));
    }
}