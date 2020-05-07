package com.griddynamics.bigdataqa.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import static com.griddynamics.bigdataqa.spark.utils.DatasetUtils.readDataFrameFromFile;
import static org.hamcrest.core.Is.is;

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

        Dataset<Tuple2<Row, Row>> tuple2Dataset = rowDataset.joinWith(rowDataset2, rowDataset.col("id").equalTo(rowDataset2.col("id"))
                .and(((rowDataset.col("sum").minus(rowDataset2.col("sum")))
                        .divide(rowDataset2.col("sum"))).multiply(100)
                        .$greater(10).or(
                                ((rowDataset2.col("sum").minus(rowDataset.col("sum")))
                                        .divide(rowDataset2.col("sum"))).multiply(100)
                                        .$greater(10))), "inner");
        Assert.assertThat(tuple2Dataset.count(), is(2L));
        Assert.assertThat(tuple2Dataset.filter(filterFunction("0f79c2f1-0946-47bc-8f6f-fdaf9134ecce")).count(), is(1L));
        Assert.assertThat(tuple2Dataset.filter(filterFunction("ec59a4ab-4794-4f2d-8fa4-4f9d45eef4af")).count(), is(1L));
    }

    private FilterFunction<Tuple2<Row, Row>> filterFunction(String id) {
        return (FilterFunction<Tuple2<Row, Row>>) rowRowTuple2 ->
                rowRowTuple2._1.getString(0).contains(id);
    }
}