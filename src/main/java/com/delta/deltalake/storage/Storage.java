package com.delta.deltalake.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Storage {

    byte[] read(String key) throws IOException;

    void write(String key, byte[] data) throws IOException;

    void write(String key, Path source) throws IOException;

    boolean create(String key, byte[] data) throws IOException;

    boolean exists(String key) throws IOException;

    List<String> list(String prefix) throws IOException;
}