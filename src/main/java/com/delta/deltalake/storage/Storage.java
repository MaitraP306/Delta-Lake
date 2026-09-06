package com.delta.deltalake.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
/** Storage abstraction used by the table layer for local filesystems and object stores. */
public interface Storage {
    byte[] read(String key) throws IOException;
    void write(String key, byte[] data) throws IOException;
    void write(String key, Path source) throws IOException;
    boolean create(String key, byte[] data) throws IOException;
    boolean exists(String key) throws IOException;
    List<String> list(String prefix) throws IOException;
    List<String> listAfter(String prefix, String startAfter) throws IOException;
    void delete(String key) throws IOException;
    default boolean supportsEventualConsistency() { return false; }
    default long size(String key) throws IOException { return read(key).length; }
    default long modificationTimeMillis(String key) throws IOException { return 0L; }
}
