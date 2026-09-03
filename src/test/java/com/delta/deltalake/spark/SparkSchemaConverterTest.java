
package com.delta.deltalake.spark;

import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SparkSchemaConverterTest {
    @Test
    void roundTripsPrimitiveAndNestedSparkSchemaToAvroBackToSpark() {
        StructType nested = new StructType(new StructField[]{
                DataTypes.createStructField("flag", DataTypes.BooleanType, true),
                DataTypes.createStructField("score", DataTypes.DoubleType, true)
        });
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("tags", DataTypes.createArrayType(DataTypes.StringType, true), true),
                DataTypes.createStructField("attrs", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true),
                DataTypes.createStructField("nested", nested, true),
                DataTypes.createStructField("amount", DataTypes.createDecimalType(12, 2), true),
                DataTypes.createStructField("when", DataTypes.TimestampType, true),
                DataTypes.createStructField("day", DataTypes.DateType, true)
        });
        var tableSchema = SparkSchemaConverter.toTableSchema(sparkSchema);
        StructType converted = SparkSchemaConverter.toSparkSchema(tableSchema);
        assertArrayEquals(sparkSchema.fieldNames(), converted.fieldNames());
        assertEquals("long", converted.apply("id").dataType().typeName());
        assertEquals("array", converted.apply("tags").dataType().typeName());
        assertEquals("map", converted.apply("attrs").dataType().typeName());
        assertEquals("struct", converted.apply("nested").dataType().typeName());
        assertEquals("decimal(12,2)", converted.apply("amount").dataType().catalogString());
    }

    @Test
    void rejectsNonStringSparkMapKeysForAvroCompatibility() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("badMap", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType, true), true)
        });
        assertThrows(IllegalArgumentException.class, () -> SparkSchemaConverter.toTableSchema(schema));
    }
}
