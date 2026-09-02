package com.delta.deltalake.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class LocalStorage implements Storage {
    private final Path root;

    public LocalStorage(Path root) {
        this.root = Objects.requireNonNull(root).toAbsolutePath().normalize();
    }

    public Path root() {
        return root;
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
    public void write(String key, Path source) throws IOException {
        Path destination = resolve(key);
        Files.createDirectories(destination.getParent());
        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public boolean create(String key, byte[] data) throws IOException {
        Path path = resolve(key);
        Files.createDirectories(path.getParent());
        try {
            Files.write(path, data, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        return Files.exists(resolve(key));
    }

    @Override
    public List<String> list(String prefix) throws IOException {
        Path base = resolve(prefix);
        if (!Files.exists(base)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.walk(base)) {
            return stream.filter(Files::isRegularFile).map(root::relativize).map(Path::toString).sorted().toList();
        }
    }

    @Override
    public List<String> listAfter(String prefix, String startAfter) throws IOException {
        Path directory = resolve(prefix);
        if (!Files.exists(directory)) {
            return List.of();
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException("Storage prefix is not a directory: " + prefix);
        }
        List<String> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path path : stream) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }
                String key = root.relativize(path).toString().replace(File.separator, "/");

                if (key.compareTo(startAfter) > 0) {
                    result.add(key);
                }
            }
        }
        result.sort(String::compareTo);
        return result;
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(resolve(key));
    }

    private Path resolve(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank() || key.startsWith("/") || key.contains("\\")) {
            throw new IllegalArgumentException("Invalid storage key: " + key);
        }
        Path resolved = root.resolve(key).normalize();
        if (!resolved.startsWith(root)) {
            throw new IllegalArgumentException("Storage key escapes root: " + key);
        }
        return resolved;
    }
}
