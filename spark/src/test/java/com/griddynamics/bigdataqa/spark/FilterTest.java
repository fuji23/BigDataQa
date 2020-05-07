package com.griddynamics.bigdataqa.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.griddynamics.bigdataqa.spark.utils.DatasetUtils.readDataFrameFromFile;

/**
 * @author adanchenko on 2020-05-01
 */
public class FilterTest extends SharedJavaSparkContext {
    SparkSession session;

    @Before
    public void setup() {
        session = SparkSession.builder().appName("test_app")
                .master("local[2]")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        session.cloneSession();
    }

    @Test
    public void testFilter() {
        Dataset<Row> rowDataset = readDataFrameFromFile(session,
                "src/test/resources/test_data.tsv");
        Dataset<Row> rowDataset2 = readDataFrameFromFile(session,
                "src/test/resources/test_data2.tsv");

        rowDataset.joinWith(rowDataset2, rowDataset.col("id").equalTo(rowDataset2.col("id"))
                .and(((rowDataset.col("sum").minus(rowDataset2.col("sum")))
                        .divide(rowDataset2.col("sum"))).multiply(100)
                        .$greater(10).or(
                                ((rowDataset2.col("sum").minus(rowDataset.col("sum")))
                                        .divide(rowDataset2.col("sum"))).multiply(100)
                                        .$greater(10))), "inner").show();
    }
}