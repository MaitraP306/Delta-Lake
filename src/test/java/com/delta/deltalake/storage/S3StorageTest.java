
package com.delta.deltalake.storage;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class S3StorageTest {

    @Test
    void mapsKeysAndSupportsAtomicCreate() throws Exception {
        FakeClient fake = new FakeClient();
        try (S3Storage storage = new S3Storage("bucket", "tables/demo", fake)) {
            assertTrue(storage.create("_delta_log/00000000000000000000.json", "a".getBytes(StandardCharsets.UTF_8)));
            assertFalse(storage.create("_delta_log/00000000000000000000.json", "b".getBytes(StandardCharsets.UTF_8)));
            assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), storage.read("_delta_log/00000000000000000000.json"));
            assertFalse(storage.supportsEventualConsistency());
            assertEquals(List.of("_delta_log/00000000000000000000.json"), storage.list("_delta_log"));
        }
    }

    @Test
    void listAfterIsLexicographicallyOrderedAndRelativeToPrefix() throws Exception {
        FakeClient fake = new FakeClient();
        try (S3Storage storage = new S3Storage("bucket", "table", fake)) {
            storage.write("a/00000000000000000000.json", new byte[]{0});
            storage.write("a/00000000000000000002.json", new byte[]{0});
            storage.write("a/00000000000000000001.json", new byte[]{0});
            assertEquals(List.of("a/00000000000000000001.json", "a/00000000000000000002.json"), storage.listAfter("a", "a/00000000000000000000.json"));
        }
    }

    @Test
    void sizeAndModificationTimeComeFromObjectStore() throws Exception {
        FakeClient fake = new FakeClient();
        try (S3Storage storage = new S3Storage("bucket", "", fake)) {
            storage.write("x", new byte[]{1,2,3});
            assertEquals(3, storage.size("x"));
            assertTrue(storage.modificationTimeMillis("x") > 0);
        }
    }

    private static final class FakeClient implements S3ObjectStoreClient {
        private record Obj(byte[] data, long modified) {}
        private final Map<String, Obj> objects = new TreeMap<>();

        @Override public byte[] read(String bucket, String key) {
            Obj obj = objects.get(key);
            if (obj == null) throw NoSuchKeyException.builder().build();
            return obj.data.clone();
        }
        @Override public void write(String bucket, String key, byte[] data) {
            objects.put(key, new Obj(data.clone(), System.currentTimeMillis()));
        }
        @Override public void write(String bucket, String key, Path source) throws java.io.IOException {
            write(bucket, key, Files.readAllBytes(source));
        }
        @Override public boolean create(String bucket, String key, byte[] data) {
            if (objects.containsKey(key)) return false;
            objects.put(key, new Obj(data.clone(), System.currentTimeMillis()));
            return true;
        }
        @Override public boolean exists(String bucket, String key) {
            return objects.containsKey(key);
        }
        @Override public List<String> list(String bucket, String prefix, String startAfter) {
            return objects.keySet().stream().filter(k -> k.startsWith(prefix) && (startAfter == null || k.compareTo(startAfter) > 0)).toList();
        }
        @Override public void delete(String bucket, String key) { objects.remove(key); }
        @Override public long size(String bucket, String key) { return objects.get(key).data.length; }
        @Override public long modificationTimeMillis(String bucket, String key) { return objects.get(key).modified; }
    }
}
