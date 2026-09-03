
package com.delta.deltalake.storage;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


interface S3ObjectStoreClient {
    byte[] read(String bucket, String key) throws IOException;
    void write(String bucket, String key, byte[] data) throws IOException;
    void write(String bucket, String key, Path source) throws IOException;
    boolean create(String bucket, String key, byte[] data) throws IOException;
    boolean exists(String bucket, String key) throws IOException;
    List<String> list(String bucket, String prefix, String startAfter) throws IOException;
    void delete(String bucket, String key) throws IOException;
    long size(String bucket, String key) throws IOException;
    long modificationTimeMillis(String bucket, String key) throws IOException;
}

final class AwsS3ObjectStoreClient implements S3ObjectStoreClient {
    private final S3Client client;
    private final int maxAttempts;
    private final Duration initialBackoff;

    AwsS3ObjectStoreClient(S3Client client) {
        this(client, 4, Duration.ofMillis(100));
    }

    AwsS3ObjectStoreClient(S3Client client, int maxAttempts, Duration initialBackoff) {
        this.client = Objects.requireNonNull(client);
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be > 0");
        if (initialBackoff.isNegative()) throw new IllegalArgumentException("initialBackoff must be non-negative");
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
    }

    @Override
    public byte[] read(String bucket, String key) throws IOException {
        return withRetry(() -> {
            ResponseBytes<GetObjectResponse> response = client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build());
            return response.asByteArray();
        }, true);
    }

    @Override
    public void write(String bucket, String key, byte[] data) throws IOException {
        withRetry(() -> {
            client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromBytes(data));
            return null;
        }, false);
    }

    @Override
    public void write(String bucket, String key, Path source) throws IOException {
        withRetry(() -> {
            client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(source));
            return null;
        }, false);
    }

    @Override
    public boolean create(String bucket, String key, byte[] data) throws IOException {
        try {
            client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build(), RequestBody.fromBytes(data));
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 412 || e.statusCode() == 409) return false;
            throw e;
        }
    }

    @Override
    public boolean exists(String bucket, String key) throws IOException {
        try {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false;
            throw e;
        }
    }

    @Override
    public List<String> list(String bucket, String prefix, String startAfter) throws IOException {
        List<String> result = new ArrayList<>();
        String token = null;
        do {
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1000);
            if (token != null) builder.continuationToken(token);
            if (startAfter != null) builder.startAfter(startAfter);
            ListObjectsV2Response response = client.listObjectsV2(builder.build());
            for (S3Object object : response.contents()) result.add(object.key());
            token = response.nextContinuationToken();
        } while (token != null);
        result.sort(String::compareTo);
        return result;
    }

    @Override
    public long size(String bucket, String key) throws IOException {
        return withRetry(() -> client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).contentLength(), true);
    }

    @Override
    public long modificationTimeMillis(String bucket, String key) throws IOException {
        return withRetry(() -> {
            var modified = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).lastModified();
            return modified == null ? 0L : modified.toEpochMilli();
        }, true);
    }

    @Override
    public void delete(String bucket, String key) throws IOException {
        withRetry(() -> {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            return null;
        }, false);
    }

    private <T> T withRetry(IoSupplier<T> supplier, boolean retryNotFound) throws IOException {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return supplier.get();
            } catch (NoSuchKeyException | NoSuchBucketException e) {
                last = e;
                if (!retryNotFound || attempt == maxAttempts) throw e;
            } catch (S3Exception e) {
                if (!(e.statusCode() == 404 && retryNotFound) && e.statusCode() < 500) throw e;
                last = e;
            } catch (RuntimeException e) {
                if (attempt == maxAttempts) throw e;
                last = e;
            }
            sleep(attempt);
        }
        throw last == null ? new IOException("S3 operation failed") : new IOException("S3 operation failed after retries", last);
    }

    private void sleep(int attempt) throws IOException {
        if (initialBackoff.isZero()) return;
        try {
            Thread.sleep(Math.min(2000L, initialBackoff.toMillis() * (1L << Math.min(attempt - 1, 5))));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying S3 operation", e);
        }
    }

    @FunctionalInterface
    private interface IoSupplier<T> { T get() throws IOException; }
}
