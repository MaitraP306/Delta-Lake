package com.delta.deltalake.data;

import org.apache.avro.Schema;
import org.apache.avro.LogicalType;

import java.util.*;

public final class TableSchema {
    private final Schema avroSchema;

    public TableSchema(Schema avroSchema) {
        this.avroSchema = Objects.requireNonNull(avroSchema);
    }

    public Schema avroSchema() {
        return avroSchema;
    }

    public String json() {
        return avroSchema.toString();
    }

    public List<String> fieldNames() {
        return avroSchema.getFields().stream().map(Schema.Field::name).toList();
    }

    public Schema.Field field(String name) {
        return avroSchema.getField(name);
    }

    public static TableSchema fromJson(String schemaJson) {
        Objects.requireNonNull(schemaJson);
        return new TableSchema(new Schema.Parser().parse(schemaJson));
    }

    public boolean isCompatibleEvolutionFrom(TableSchema previous) {
        Objects.requireNonNull(previous);
        Schema oldSchema = previous.avroSchema;
        if (!oldSchema.getFullName().equals(avroSchema.getFullName())) return false;

        Set<String> matchedOld = new HashSet<>();
        for (Schema.Field newField : avroSchema.getFields()) {
            Schema.Field oldField = oldSchema.getField(newField.name());
            if (oldField == null) {
                for (String alias : newField.aliases()) {
                    oldField = oldSchema.getField(alias);
                    if (oldField != null) break;
                }
            }
            if (oldField == null) {
                if (!allowsNull(newField.schema()) && !newField.hasDefaultValue()) return false;
                continue;
            }
            if (!matchedOld.add(oldField.name())) return false;
            if (!isTypeEvolutionCompatible(oldField.schema(), newField.schema())) return false;
        }
        return true;
    }


    public boolean requiresPhysicalRewriteFrom(TableSchema previous) {
        Objects.requireNonNull(previous);
        for (Schema.Field oldField : previous.avroSchema.getFields()) {
            boolean retained = avroSchema.getField(oldField.name()) != null || avroSchema.getFields().stream().anyMatch(field -> field.aliases().contains(oldField.name()));
            if (!retained) return true;
        }
        return false;
    }

    public String resolvePreviousFieldName(Schema.Field newField, TableSchema previous) {
        if (previous.avroSchema.getField(newField.name()) != null) return newField.name();
        for (String alias : newField.aliases()) {
            if (previous.avroSchema.getField(alias) != null) return alias;
        }
        return null;
    }

    public static boolean isTypeEvolutionCompatible(Schema oldSchema, Schema newSchema) {
        Schema oldBase = unwrapNullable(oldSchema);
        Schema newBase = unwrapNullable(newSchema);
        if (oldBase.equals(newBase)) return true;

        LogicalType oldLogical = oldBase.getLogicalType();
        LogicalType newLogical = newBase.getLogicalType();
        if (oldLogical != null || newLogical != null) {
            if (oldLogical == null || newLogical == null || !oldLogical.getName().equals(newLogical.getName())) return false;
            if ("decimal".equals(oldLogical.getName())) {
                int oldScale = ((org.apache.avro.LogicalTypes.Decimal) oldLogical).getScale();
                int newScale = ((org.apache.avro.LogicalTypes.Decimal) newLogical).getScale();
                int oldPrecision = ((org.apache.avro.LogicalTypes.Decimal) oldLogical).getPrecision();
                int newPrecision = ((org.apache.avro.LogicalTypes.Decimal) newLogical).getPrecision();
                return oldScale == newScale && newPrecision >= oldPrecision;
            }
        }

        return switch (oldBase.getType()) {
            case INT -> newBase.getType() == Schema.Type.LONG || newBase.getType() == Schema.Type.FLOAT || newBase.getType() == Schema.Type.DOUBLE;
            case LONG -> newBase.getType() == Schema.Type.FLOAT || newBase.getType() == Schema.Type.DOUBLE;
            case FLOAT -> newBase.getType() == Schema.Type.DOUBLE;
            case ARRAY -> newBase.getType() == Schema.Type.ARRAY && isTypeEvolutionCompatible(oldBase.getElementType(), newBase.getElementType());
            case MAP -> newBase.getType() == Schema.Type.MAP && isTypeEvolutionCompatible(oldBase.getValueType(), newBase.getValueType());
            case RECORD -> recordsCompatible(oldBase, newBase);
            default -> false;
        };
    }

    private static boolean recordsCompatible(Schema oldRecord, Schema newRecord) {
        if (oldRecord.getType() != Schema.Type.RECORD || newRecord.getType() != Schema.Type.RECORD) return false;
        for (Schema.Field oldField : oldRecord.getFields()) {
            Schema.Field newField = newRecord.getField(oldField.name());
            if (newField == null) {
                newField = newRecord.getFields().stream().filter(f -> f.aliases().contains(oldField.name())).findFirst().orElse(null);
            }
            if (newField == null) return false;
            if (!isTypeEvolutionCompatible(oldField.schema(), newField.schema())) return false;
        }
        for (Schema.Field newField : newRecord.getFields()) {
            if (oldRecord.getField(newField.name()) == null && !newField.hasDefaultValue() && !allowsNull(newField.schema())) return false;
        }
        return true;
    }

    private static boolean containsNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION && schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private static boolean allowsNull(Schema schema) {
        return containsNullable(schema);
    }

    public static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        for (Schema type : schema.getTypes()) if (type.getType() != Schema.Type.NULL) return type;
        throw new IllegalArgumentException("Union schema contains only null");
    }
}
