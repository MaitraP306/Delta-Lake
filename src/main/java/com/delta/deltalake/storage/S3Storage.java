 
package com.delta.deltalake.storage;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.net.URI;
import java.util.List;
import java.util.Objects;

public final class S3Storage implements Storage, AutoCloseable {
    private final String bucket;
    private final String prefix;
    private final S3ObjectStoreClient client;
    private final S3Client ownedClient;

    public S3Storage(String bucket, String prefix, Region region) {
        this(bucket, prefix, region, null);
    }

    public S3Storage(String bucket, String prefix, Region region, URI endpointOverride) {
        this(bucket, prefix, buildClient(region, endpointOverride), true);
    }

    private static S3Client buildClient(Region region, URI endpointOverride) {
        var builder = S3Client.builder().region(Objects.requireNonNull(region)).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(endpointOverride != null).build());
        if (endpointOverride != null) builder = builder.endpointOverride(endpointOverride);
        return builder.build();
    }

    public S3Storage(String bucket, String prefix, S3Client client) {
        this(bucket, prefix, client, false);
    }

    S3Storage(String bucket, String prefix, S3ObjectStoreClient client) {
        this.bucket = validateBucket(bucket);
        this.prefix = normalizePrefix(prefix);
        this.client = Objects.requireNonNull(client);
        this.ownedClient = null;
    }

    private S3Storage(String bucket, String prefix, S3Client client, boolean ownClient) {
        this.bucket = validateBucket(bucket);
        this.prefix = normalizePrefix(prefix);
        this.client = new AwsS3ObjectStoreClient(Objects.requireNonNull(client));
        this.ownedClient = ownClient ? client : null;
    }

    public String bucket() { return bucket; }
    public String prefix() { return prefix; }

    @Override
    public byte[] read(String key) throws IOException {
        return client.read(bucket, fullKey(key));
    }

    @Override
    public void write(String key, byte[] data) throws IOException {
        client.write(bucket, fullKey(key), data);
    }

    @Override
    public void write(String key, Path source) throws IOException {
        client.write(bucket, fullKey(key), source);
    }

    @Override
    public boolean create(String key, byte[] data) throws IOException {
        return client.create(bucket, fullKey(key), data);
    }

    @Override
    public boolean exists(String key) throws IOException {
        return client.exists(bucket, fullKey(key));
    }

    @Override
    public List<String> list(String prefix) throws IOException {
        String normalized = normalizeKey(prefix);
        String actualPrefix = fullKey(normalized);
        return client.list(bucket, actualPrefix, null).stream().map(this::relativeKey).toList();
    }

    @Override
    public List<String> listAfter(String prefix, String startAfter) throws IOException {
        String normalizedPrefix = normalizeKey(prefix);
        String actualPrefix = fullKey(normalizedPrefix);
        String actualStart = startAfter == null ? null : fullKey(startAfter);
        return client.list(bucket, actualPrefix, actualStart).stream().filter(k -> startAfter == null || relativeKey(k).compareTo(normalizeKey(startAfter)) > 0).map(this::relativeKey).toList();
    }

    @Override
    public boolean supportsEventualConsistency() { return false; }

    @Override
    public long size(String key) throws IOException { return client.size(bucket, fullKey(key)); }

    @Override
    public long modificationTimeMillis(String key) throws IOException { return client.modificationTimeMillis(bucket, fullKey(key)); }

    @Override
    public void delete(String key) throws IOException {
        client.delete(bucket, fullKey(key));
    }

    private String fullKey(String key) {
        String normalized = normalizeKey(key);
        return prefix.isEmpty() ? normalized : prefix + "/" + normalized;
    }

    private String relativeKey(String key) {
        if (prefix.isEmpty()) return key;
        String p = prefix + "/";
        if (!key.startsWith(p)) throw new IllegalStateException("S3 key outside configured prefix: " + key);
        return key.substring(p.length());
    }

    private static String normalizePrefix(String value) {
        if (value == null || value.isBlank()) return "";
        String normalized = value.replace('\\', '/');
        while (normalized.startsWith("/")) normalized = normalized.substring(1);
        while (normalized.endsWith("/")) normalized = normalized.substring(0, normalized.length() - 1);
        if (normalized.contains("..")) throw new IllegalArgumentException("Invalid S3 prefix: " + value);
        return normalized;
    }

    private static String normalizeKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank() || key.startsWith("/") || key.contains("\\")) throw new IllegalArgumentException("Invalid storage key: " + key);
        if (key.contains("../") || key.equals("..") || key.startsWith("../")) throw new IllegalArgumentException("Storage key escapes root: " + key);
        return key.replace('\\', '/');
    }

    private static String validateBucket(String bucket) {
        if (bucket == null || bucket.isBlank()) throw new IllegalArgumentException("bucket cannot be blank");
        return bucket;
    }

    @Override
    public void close() {
        if (ownedClient != null) ownedClient.close();
    }

}
 