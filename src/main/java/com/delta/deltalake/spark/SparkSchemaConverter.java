
package com.delta.deltalake.spark;

import com.delta.deltalake.data.TableSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class SparkSchemaConverter {
    private SparkSchemaConverter() {}

    public static TableSchema toTableSchema(StructType sparkSchema) {
        Objects.requireNonNull(sparkSchema);
        Schema record = Schema.createRecord("SparkRow", null, "com.delta.deltalake.spark", false);
        List<Schema.Field> fields = new ArrayList<>();
        for (StructField field : sparkSchema.fields()) {
            fields.add(new Schema.Field(field.name(), toAvro(field.dataType(), field.name(), field.nullable()), null, null));
        }
        record.setFields(fields);
        return new TableSchema(record);
    }

    private static Schema toAvro(DataType type, String name, boolean nullable) {
        Schema base;

        if (type instanceof DecimalType decimal) {
            base = org.apache.avro.LogicalTypes.decimal(decimal.precision(), decimal.scale()).addToSchema(Schema.create(Schema.Type.BYTES));

        } else if (type instanceof ArrayType array) {
            base = Schema.createArray(toAvro(array.elementType(), name + "Element", array.containsNull()));

        } else if (type instanceof MapType map) {
            if (!map.keyType().sameType(DataTypes.StringType)) {
                throw new IllegalArgumentException("Spark map keys must be strings for Avro-backed Delta rows: " + name);
            }

            base = Schema.createMap(toAvro(map.valueType(), name + "Value", map.valueContainsNull()));

        } else if (type instanceof StructType struct) {
            Schema nested = Schema.createRecord(safeName(name), null, "com.delta.deltalake.spark", false);
            List<Schema.Field> fields = new ArrayList<>();
            for (StructField child : struct.fields()) {
                fields.add(new Schema.Field(child.name(), toAvro(child.dataType(), name + "_" + child.name(), child.nullable()), null, null));
            }
            nested.setFields(fields);
            base = nested;
        } else {
            String kind = type.typeName().toLowerCase(Locale.ROOT);

            base = switch (kind) {
                case "string" -> Schema.create(Schema.Type.STRING);
                case "boolean" -> Schema.create(Schema.Type.BOOLEAN);
                case "byte", "short", "integer" -> Schema.create(Schema.Type.INT);
                case "long" -> Schema.create(Schema.Type.LONG);
                case "float" -> Schema.create(Schema.Type.FLOAT);
                case "double" -> Schema.create(Schema.Type.DOUBLE);
                case "binary" -> Schema.create(Schema.Type.BYTES);
                case "date" -> org.apache.avro.LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                case "timestamp", "timestamp_ntz" -> org.apache.avro.LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                default -> throw new IllegalArgumentException("Unsupported Spark SQL type: " + type.catalogString());
            };
        }
        return nullable ? nullable(base) : base;
    }

    public static StructType toSparkSchema(TableSchema tableSchema) {
        List<StructField> fields = new ArrayList<>();
        for (Schema.Field field : tableSchema.avroSchema().getFields()) {
            fields.add(DataTypes.createStructField(field.name(), toSparkType(field.schema()), isNullable(field.schema())));
        }
        return DataTypes.createStructType(fields);
    }

    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION && schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.NULL);
    }

    private static DataType toSparkType(Schema schema) {
        Schema base = TableSchema.unwrapNullable(schema);
        LogicalType logical = base.getLogicalType();
        if (logical != null) {
            return switch (logical.getName()) {
                case "decimal" -> {
                    if (logical instanceof org.apache.avro.LogicalTypes.Decimal decimal) {
                        yield DataTypes.createDecimalType(decimal.getPrecision(), decimal.getScale());
                    }
                    yield DataTypes.createDecimalType(38, 18);
                }
                case "date" -> DataTypes.DateType;
                case "timestamp-millis", "timestamp-micros" -> DataTypes.TimestampType;
                default -> toSparkPrimitive(base);
            };
        }
        return toSparkPrimitive(base);
    }

    private static DataType toSparkPrimitive(Schema schema) {
        return switch (schema.getType()) {
            case STRING -> DataTypes.StringType;
            case BOOLEAN -> DataTypes.BooleanType;
            case INT -> DataTypes.IntegerType;
            case LONG -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case BYTES -> DataTypes.BinaryType;
            case ARRAY -> DataTypes.createArrayType(toSparkType(schema.getElementType()), true);
            case MAP -> DataTypes.createMapType(DataTypes.StringType, toSparkType(schema.getValueType()), true);
            case RECORD -> {
                List<StructField> fields = new ArrayList<>();
                for (Schema.Field field : schema.getFields()) {
                    fields.add(DataTypes.createStructField(field.name(), toSparkType(field.schema()), true));
                }
                yield DataTypes.createStructType(fields);
            }
            default -> throw new IllegalArgumentException("Unsupported Avro type for Spark: " + schema.getType());
        };
    }

    private static Schema nullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) return schema;
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    private static String safeName(String name) {
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
