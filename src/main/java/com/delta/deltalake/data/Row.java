package com.delta.deltalake.data;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public final class Row {
    private final TableSchema schema;
    private final Map<String, Object> values;

    private Row(TableSchema schema, Map<String, ?> values) {
        this.schema = Objects.requireNonNull(schema);
        Objects.requireNonNull(values);
        LinkedHashMap<String, Object> copy = new LinkedHashMap<>();
        for (Schema.Field field : schema.avroSchema().getFields()) {
            if (values.containsKey(field.name())) {
                copy.put(field.name(), normalize(values.get(field.name())));
            }
        }
        for (String name : values.keySet()) {
            if (schema.avroSchema().getField(name) == null) {
                throw new IllegalArgumentException("Unknown column: " + name);
            }
        }
        this.values = Collections.unmodifiableMap(copy);
    }

    public static Row of(TableSchema schema, Map<String, ?> values) {
        return new Row(schema, values);
    }

    public static Row infer(Map<String, ?> values) {
        Objects.requireNonNull(values);
        Schema record = Schema.createRecord("Row", null, "com.delta.deltalake.data", false);
        List<Schema.Field> fields = new ArrayList<>();
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            fields.add(new Schema.Field(entry.getKey(), inferType(entry.getKey(), entry.getValue()), null, null));
        }
        record.setFields(fields);
        return new Row(new TableSchema(record), values);
    }

    private static Schema inferType(String name, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot infer type for null column: " + name);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return Schema.create(Schema.Type.INT);
        }
        if (value instanceof Long) return Schema.create(Schema.Type.LONG);
        if (value instanceof BigInteger integer) return decimalSchema(new BigDecimal(integer).precision(), 0);
        if (value instanceof Float) return Schema.create(Schema.Type.FLOAT);
        if (value instanceof Double) return Schema.create(Schema.Type.DOUBLE);
        if (value instanceof BigDecimal decimal) {
            int precision = Math.max(1, decimal.precision());
            return decimalSchema(precision, Math.max(0, decimal.scale()));
        }
        if (value instanceof Boolean) return Schema.create(Schema.Type.BOOLEAN);
        if (value instanceof CharSequence || value instanceof java.util.UUID) return Schema.create(Schema.Type.STRING);
        if (value instanceof ByteBuffer || value instanceof byte[]) return Schema.create(Schema.Type.BYTES);
        if (value instanceof LocalDate) return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        if (value instanceof Instant) return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        if (value instanceof List<?> list) {
            if (list.isEmpty()) throw new IllegalArgumentException("Cannot infer element type for empty array column: " + name);
            Object sample = list.stream().filter(Objects::nonNull).findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot infer element type for null-only array column: " + name));
            return Schema.createArray(inferType(name + "[]", sample));
        }
        if (value instanceof Map<?, ?> map) {
            Object sample = map.values().stream().filter(Objects::nonNull).findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot infer map value type for null-only map column: " + name));
            for (Object key : map.keySet()) {
                if (!(key instanceof CharSequence)) {
                    throw new IllegalArgumentException("Avro map keys must be strings for column: " + name);
                }
            }
            return Schema.createMap(inferType(name + "{}", sample));
        }
        if (value instanceof Row nested) return nested.schema().avroSchema();
        throw new IllegalArgumentException("Unsupported inferred column type for '" + name + "': " + value.getClass().getName());
    }

    private static Schema decimalSchema(int precision, int scale) {
        int safePrecision = Math.max(1, precision);
        int safeScale = Math.min(Math.max(0, scale), safePrecision);
        return LogicalTypes.decimal(safePrecision, safeScale).addToSchema(Schema.create(Schema.Type.BYTES));
    }

    private static Object normalize(Object value) {
        if (value instanceof Byte || value instanceof Short) return ((Number) value).intValue();
        if (value == JsonProperties.NULL_VALUE) return null;
        if (value instanceof CharSequence && !(value instanceof String)) return value.toString();
        if (value instanceof byte[] bytes) return ByteBuffer.wrap(bytes);
        return value;
    }

    public TableSchema schema() { return schema; }
    public Object get(String column) { return values.get(column); }
    public boolean contains(String column) { return values.containsKey(column); }
    public Map<String, Object> values() { return values; }

    public Row with(String column, Object value) {
        if (schema.avroSchema().getField(column) == null) {
            throw new IllegalArgumentException("Unknown column: " + column);
        }
        LinkedHashMap<String, Object> updated = new LinkedHashMap<>(values);
        updated.put(column, value);
        return new Row(schema, updated);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Row row && schema.json().equals(row.schema.json()) && values.equals(row.values);
    }

    @Override
    public int hashCode() { return Objects.hash(schema.json(), values); }

    @Override
    public String toString() { return "Row" + values; }
}
