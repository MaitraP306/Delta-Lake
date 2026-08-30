package com.delta.deltalake.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class LocalStorage implements Storage {

    private final Path root;

    public LocalStorage(Path root) {
        this.root = root;
    }

    @Override
    public byte[] read(String key) throws IOException {
        return Files.readAllBytes(resolve(key));
    }

    @Override
    public void write(String key, byte[] data) throws IOException {
        Path path = resolve(key);
        Files.createDirectories(path.getParent());
        Files.write(path, data);
    }

    @Override
    public boolean create(String key, byte[] data) throws IOException {
        Path path = resolve(key);
        Files.createDirectories(path.getParent());

        try {
            Files.write(
                    path,
                    data,
                    java.nio.file.StandardOpenOption.CREATE_NEW
            );

            return true;
        } catch (java.nio.file.FileAlreadyExistsException e) {
            return false;
        }
    }

    @Override
    public boolean exists(String key) {
        return Files.exists(resolve(key));
    }

    @Override
    public List<String> list(String prefix) throws IOException {
        Path base = resolve(prefix);

        if (!Files.exists(base)) {
            return List.of();
        }

        try (Stream<Path> stream = Files.walk(base)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(root::relativize)
                    .map(Path::toString)
                    .toList();
        }
    }

    private Path resolve(String key) {
        return root.resolve(key);
    }


}