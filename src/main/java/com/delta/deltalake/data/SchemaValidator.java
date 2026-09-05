package com.delta.deltalake.data;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

public final class SchemaValidator {
    private SchemaValidator() {}

    public static void validate(TableSchema tableSchema, GenericRecord record) {
        Schema schema = tableSchema.avroSchema();
        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.name());
            if (value == JsonProperties.NULL_VALUE) value = null;
            if (value == null) {
                if (field.hasDefaultValue() || allowsNull(field.schema())) continue;
                throw new IllegalArgumentException("Missing required field: " + field.name());
            }
            validateValue(field.name(), field.schema(), value);
        }
    }

    private static boolean allowsNull(Schema schema) {
        return schema.getType() == Schema.Type.UNION && schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private static void validateValue(String fieldName, Schema schema, Object value) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) continue;
                try {
                    validateValue(fieldName, branch, value);
                    return;
                } catch (IllegalArgumentException ignored) { }
            }
            throw expected(fieldName, schema, value);
        }

        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            switch (logicalType.getName()) {
                case "decimal" -> {
                    if (!(value instanceof ByteBuffer) && !(value instanceof BigDecimal)) throw expected(fieldName, schema, value);
                    if (value instanceof ByteBuffer) return;
                    return;
                }
                case "date" -> {
                    if (!(value instanceof Integer) && !(value instanceof LocalDate)) throw expected(fieldName, schema, value);
                    if (value instanceof Integer) return;
                }
                case "timestamp-millis" -> {
                    if (!(value instanceof Long) && !(value instanceof Instant)) throw expected(fieldName, schema, value);
                    if (value instanceof Long) return;
                }
                default -> { }
            }
        }

        switch (schema.getType()) {
            case LONG -> {
                if (!(value instanceof Number) || value instanceof Float || value instanceof Double) throw expected(fieldName, schema, value);
            }
            case INT -> {
                if (!(value instanceof Integer) && !(value instanceof Short) && !(value instanceof Byte)) throw expected(fieldName, schema, value);
            }
            case STRING -> {
                if (!(value instanceof CharSequence)) throw expected(fieldName, schema, value);
            }
            case BOOLEAN -> {
                if (!(value instanceof Boolean)) throw expected(fieldName, schema, value);
            }
            case DOUBLE, FLOAT -> {
                if (!(value instanceof Number)) throw expected(fieldName, schema, value);
            }
            case BYTES -> {
                if (!(value instanceof ByteBuffer) && !(value instanceof byte[])) throw expected(fieldName, schema, value);
            }
            case ARRAY -> {
                if (!(value instanceof Iterable<?> iterable)) throw expected(fieldName, schema, value);
                for (Object element : iterable) validateNullableValue(fieldName + "[]", schema.getElementType(), element);
            }
            case MAP -> {
                if (!(value instanceof Map<?, ?> map)) throw expected(fieldName, schema, value);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof CharSequence)) throw new IllegalArgumentException("Map key for field '" + fieldName + "' must be a string");
                    validateNullableValue(fieldName + "{}", schema.getValueType(), entry.getValue());
                }
            }
            case RECORD -> {
                if (!(value instanceof GenericRecord record) && !(value instanceof Map<?, ?>)) throw expected(fieldName, schema, value);
                for (Schema.Field nested : schema.getFields()) {
                    Object nestedValue = value instanceof GenericRecord record ? record.get(nested.name()) : ((Map<?, ?>) value).get(nested.name());
                    if (nestedValue == null) {
                        if (nested.hasDefaultValue() || allowsNull(nested.schema())) continue;
                        throw new IllegalArgumentException("Missing required field: " + fieldName + "." + nested.name());
                    }
                    validateValue(fieldName + "." + nested.name(), nested.schema(), nestedValue);
                }
            }
            case NULL -> { }
            default -> throw new UnsupportedOperationException("Unsupported schema type for field '" + fieldName + "': " + schema.getType());
        }
    }

    private static void validateNullableValue(String fieldName, Schema schema, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE) {
            if (!allowsNull(schema)) throw new IllegalArgumentException("Missing required value for field '" + fieldName + "'");
            return;
        }
        validateValue(fieldName, schema, value);
    }

    private static IllegalArgumentException expected(String fieldName, Schema schema, Object value) {
        return new IllegalArgumentException("Field '" + fieldName + "' expected " + schema.getType() + " but got " + value.getClass().getSimpleName());
    }
}
