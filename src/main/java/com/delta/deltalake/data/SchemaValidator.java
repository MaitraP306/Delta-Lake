package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public final class SchemaValidator {

    private SchemaValidator() {}

    public static void validate(TableSchema tableSchema, GenericRecord record) {
        Schema schema = tableSchema.avroSchema();
        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.name());
            if (value == null) {
                if (field.hasDefaultValue()) {
                    continue;
                }
                if (allowsNull(field.schema())) {
                    continue;
                }
                throw new IllegalArgumentException("Missing required field: " + field.name());
            }
            validateType(field, value);
        }
    }

    private static boolean allowsNull(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return false;
        }
        return schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private static void validateType(Schema.Field field, Object value) {
        Schema schema = unwrapNullable(field.schema());
        boolean valid = switch (schema.getType()) {
            case LONG -> value instanceof Long;
            case INT -> value instanceof Integer;
            case STRING -> value instanceof CharSequence;
            case BOOLEAN -> value instanceof Boolean;
            case DOUBLE -> value instanceof Double;
            case FLOAT -> value instanceof Float;
            case BYTES -> value instanceof java.nio.ByteBuffer;
            default -> throw new UnsupportedOperationException(
                    "Unsupported schema type for field '"
                            + field.name()
                            + "': "
                            + schema.getType()
            );
        };

        if (!valid) {
            throw new IllegalArgumentException("Field '" + field.name() + "' expected " + schema.getType() + " but got " + value.getClass().getSimpleName());
        }
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }

        for (Schema type : schema.getTypes()) {
            if (type.getType() != Schema.Type.NULL) {
                return type;
            }
        }

        throw new IllegalArgumentException("Union schema contains only null");
    }
}