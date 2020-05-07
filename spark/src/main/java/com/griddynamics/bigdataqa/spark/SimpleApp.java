package com.griddynamics.bigdataqa.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.griddynamics.bigdataqa.spark.utils.DatasetUtils.readDataFrameFromFile;

/**
 * @author adanchenko on 2020-05-04
 */
public class SimpleApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();


        Dataset<Row> rowDataset = readDataFrameFromFile(spark, args[0]);
        Dataset<Row> rowDataset2 = readDataFrameFromFile(spark, args[1]);

        rowDataset.joinWith(rowDataset2, rowDataset.col("id").equalTo(rowDataset2.col("id"))
                .and(((rowDataset.col("sum").minus(rowDataset2.col("sum")))
                        .divide(rowDataset2.col("sum"))).multiply(100)
                        .$greater(10).or(
                                ((rowDataset2.col("sum").minus(rowDataset.col("sum")))
                                        .divide(rowDataset2.col("sum"))).multiply(100)
                                        .$greater(10))), "inner").show(false);

        spark.stop();
    }
}
