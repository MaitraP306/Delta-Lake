package com.delta.deltalake.data;

import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RowCodec {
    private RowCodec() {}

    public static GenericRecord encode(Row row, TableSchema targetSchema) {
        Schema schema = targetSchema.avroSchema();
        GenericRecord value = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object fieldValue = row.contains(field.name()) ? row.get(field.name()) : null;
            if (fieldValue == null && field.hasDefaultValue()) {
                fieldValue = normalizeAvroDefault(field.defaultVal());
            }
            value.put(field.name(), encodeValue(field.schema(), fieldValue));
        }
        SchemaValidator.validate(targetSchema, value);
        return value;
    }

    public static Row decode(GenericRecord value) {
        TableSchema schema = new TableSchema(value.getSchema());
        Map<String, Object> values = new LinkedHashMap<>();
        for (Schema.Field field : value.getSchema().getFields()) {
            values.put(field.name(), decodeValue(field.schema(), value.get(field.name())));
        }
        return Row.of(schema, values);
    }

    private static Object normalizeAvroDefault(Object value) {
        return value == JsonProperties.NULL_VALUE ? null : value;
    }

    private static Object encodeValue(Schema schema, Object value) {
        if (value == null) return null;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) continue;
                try {
                    return encodeValue(branch, value);
                } catch (IllegalArgumentException ignored) {
                    // Try the next union branch.
                }
            }
            throw new IllegalArgumentException("Value does not match union schema: " + value.getClass().getName());
        }

        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            if (logicalType.getName().equals("decimal")) {
                BigDecimal decimal = value instanceof BigDecimal d ? d : value instanceof java.math.BigInteger i ? new BigDecimal(i) : null;
                if (decimal == null) {
                    throw new IllegalArgumentException("Decimal field requires BigDecimal/BigInteger, got " + value.getClass().getName());
                }
                Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
                return conversion.toBytes(decimal, schema, logicalType);
            }
            if (logicalType.getName().equals("date")) {
                if (value instanceof LocalDate date) return Math.toIntExact(date.toEpochDay());
            }
            if (logicalType.getName().equals("timestamp-millis")) {
                if (value instanceof Instant instant) return instant.toEpochMilli();
            }
        }

        return switch (schema.getType()) {
            case INT -> value instanceof Number n ? n.intValue() : value;
            case LONG -> value instanceof Number n ? n.longValue() : value;
            case FLOAT -> value instanceof Number n ? n.floatValue() : value;
            case DOUBLE -> value instanceof Number n ? n.doubleValue() : value;
            case STRING -> value instanceof java.util.UUID ? value.toString() : value;
            case BYTES -> {
                if (value instanceof byte[] bytes) yield ByteBuffer.wrap(bytes);
                yield value;
            }
            case ARRAY -> {
                if (!(value instanceof Iterable<?> iterable)) yield value;
                List<Object> encoded = new ArrayList<>();
                for (Object element : iterable) encoded.add(encodeValue(schema.getElementType(), element));
                yield encoded;
            }
            case MAP -> {
                if (!(value instanceof Map<?, ?> map)) yield value;
                Map<String, Object> encoded = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    encoded.put(String.valueOf(entry.getKey()), encodeValue(schema.getValueType(), entry.getValue()));
                }
                yield encoded;
            }
            case RECORD -> {
                if (value instanceof GenericRecord record) yield record;
                if (value instanceof Row nested) yield encode(nested, new TableSchema(schema));
                if (value instanceof Map<?, ?> map) {
                    GenericRecord nested = new GenericData.Record(schema);
                    for (Schema.Field field : schema.getFields()) {
                        Object child = map.get(field.name());
                        if (child == null && field.hasDefaultValue()) child = normalizeAvroDefault(field.defaultVal());
                        nested.put(field.name(), encodeValue(field.schema(), child));
                    }
                    yield nested;
                }
                yield value;
            }
            default -> value;
        };
    }

    private static Object decodeValue(Schema schema, Object value) {
        if (value == null) return null;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) continue;
                try { return decodeValue(branch, value); } catch (RuntimeException ignored) { }
            }
            return value;
        }
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            if (logicalType.getName().equals("decimal") && value instanceof ByteBuffer bytes) {
                Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
                return conversion.fromBytes(bytes, schema, logicalType);
            }
            if (logicalType.getName().equals("date") && value instanceof Number n) {
                return LocalDate.ofEpochDay(n.intValue());
            }
            if (logicalType.getName().equals("timestamp-millis") && value instanceof Number n) {
                return Instant.ofEpochMilli(n.longValue());
            }
        }
        if (value instanceof CharSequence && !(value instanceof String)) {
            return value.toString();
        }

        return switch (schema.getType()) {
            case ARRAY -> {
                List<Object> decoded = new ArrayList<>();
                if (value instanceof Iterable<?> iterable) {
                    for (Object element : iterable) {
                        decoded.add(decodeValue(schema.getElementType(), element));
                    }
                }
                yield List.copyOf(decoded);
            }
            case MAP -> {
                Map<String, Object> decoded = new LinkedHashMap<>();
                if (value instanceof Map<?, ?> map) for (Map.Entry<?, ?> entry : map.entrySet()) decoded.put(entry.getKey().toString(), decodeValue(schema.getValueType(), entry.getValue()));
                yield decoded;
            }
            case RECORD -> {
                if (!(value instanceof GenericRecord record)) yield value;
                Map<String, Object> decoded = new LinkedHashMap<>();
                for (Schema.Field field : schema.getFields()) decoded.put(field.name(), decodeValue(field.schema(), record.get(field.name())));
                yield java.util.Collections.unmodifiableMap(decoded);
            }
            default -> value;
        };
    }
}
