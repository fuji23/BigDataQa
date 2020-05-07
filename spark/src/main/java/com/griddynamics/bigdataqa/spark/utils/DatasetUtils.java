package com.griddynamics.bigdataqa.spark.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author adanchenko on 2020-05-03
 */
public class DatasetUtils implements Serializable {

    public static Dataset<Row> readDataFrameFromFile(SparkSession session, String filePath) {
        JavaRDD<Row> map = session.read()
                .option("header", "false")
                .option("delimiter", "\t")
                .csv(filePath)
                .javaRDD()
                .map(row -> {
                            String jsonString = row.getString(5);
                            return RowFactory.create(row.getString(3), new JSONObject(jsonString).getInt("m1") +
                                    new JSONObject(jsonString).getInt("m2") +
                                    new JSONObject(jsonString).getInt("m3") +
                                    new JSONObject(jsonString).getInt("m4") +
                                    new JSONObject(jsonString).getInt("m5"));
                        }
                );
        return session.createDataFrame(map, DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("sum", DataTypes.IntegerType, false))));
    }
}