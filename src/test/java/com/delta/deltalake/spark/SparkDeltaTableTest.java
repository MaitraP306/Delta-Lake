
package com.delta.deltalake.spark;

import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.table.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparkDeltaTableTest {
    private static SparkSession spark;
    private static Path root;

    @BeforeAll
    static void startSpark() throws Exception {
        System.setProperty("SPARK_LOCAL_HOSTNAME", "localhost");
        System.setProperty("spark.ui.enabled", "false");
        System.setProperty("spark.driver.host", "127.0.0.1");
        System.setProperty("spark.sql.shuffle.partitions", "2");
        spark = SparkSession.builder().master("local[2]").appName("DeltaSparkTest").config("spark.ui.enabled", "false").getOrCreate();
        root = Files.createTempDirectory("delta-spark-test-");
    }

    @AfterAll
    static void stopSpark() throws Exception {
        if (spark != null) spark.stop();
        if (root != null) {
            try (var stream = Files.walk(root)) {
                stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    void convertsSparkDataFrameToDeltaAndBack() throws Exception {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });
        Dataset<org.apache.spark.sql.Row> df = spark.createDataFrame(List.of(org.apache.spark.sql.RowFactory.create(1L, "alice", 30), org.apache.spark.sql.RowFactory.create(2L, "bob", 40)), schema);

        DeltaTable table = DeltaTable.open(new LocalStorage(root.resolve("table")));
        long version = SparkDeltaTable.append(table, df);
        assertEquals(0, version);

        Dataset<org.apache.spark.sql.Row> read = SparkDeltaTable.read(spark, table);
        assertEquals(2, read.count());
        assertEquals(List.of("alice", "bob"), read.orderBy("id").select("name").as(Encoders.STRING()).collectAsList());
        SparkDeltaTable.createOrReplaceTempView(spark, table, "delta_test");
        assertEquals(1L, spark.sql("select count(*) from delta_test where age >= 40").first().getLong(0));
    }
}
