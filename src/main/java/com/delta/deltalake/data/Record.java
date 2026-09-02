package com.delta.deltalake.data;
public record Record(long id, String name, int age) {
    public Record {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must be non-empty");
        }
        if (age < 0) {
            throw new IllegalArgumentException("age must be non-negative");
        }
    }
}