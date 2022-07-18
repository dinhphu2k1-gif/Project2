package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Task {
    private SparkSession spark;

    private static final String destination = "hdfs://m1:8020/project2/result";

    private final String source = "/project2/data";

    /**
     * Lấy url đã truy cập nhiều nhất trong ngày của mỗi guid
     *
     * @param data
     */
    public void exe1(Dataset<Row> data) {
        Dataset<Row> data1 = data.groupBy("guid", "domain").count();
//        data1.show();
        Dataset<Row> data2 = data1.groupBy("guid").agg(max("count").as("max"));
//        data2.show();

        data1.createOrReplaceTempView("data1");
        data2.createOrReplaceTempView("data2");

        Dataset<Row> data3 = spark.sql("SELECT data1.guid, data1.domain FROM data1 INNER JOIN data2 ON data1.guid = data2.guid AND data1.count = data2.max");
        System.out.println("Lấy url đã truy cập nhiều nhất trong ngày của mỗi guid");
        data3.show(false);

        // lưu lại kết quả
//        data3.write().parquet(resultPath + "/exe1");
        data3.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/ex1");
    }

    /**
     * Các IP được sử dụng bởi nhiều guid nhất số guid không tính lặp lại
     *
     * @param data
     */
    public void exe2(Dataset<Row> data) {
        Dataset<Row> data1 = data.groupBy("ip").agg(count_distinct(col("guid")).as("count")).orderBy(col("count").desc());
        System.out.println("Các IP được sử dụng bởi nhiều guid nhất");
        data1.show(false);

//        data1.write().parquet(resultPath + "/exe2");
        data1.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/ex2");
    }

    /**
     * Tính các guid mà có timeCreate – cookieCreate nhỏ hơn 30 phút
     *
     * @param data
     */
    public void exe3(Dataset<Row> data) {
        Dataset<Row> data1 = data.select(col("guid")
                , col("timeCreate").cast("long").minus(col("cookieCreate").cast("long")).as("duration"));
        data1 = data1.filter(col("duration").lt(30 * 60000));
        System.out.println("Tính các guid mà có timeCreate – cookieCreate nhỏ hơn 30 phút");
        data1.show(false);

//        data1.write().parquet(resultPath + "/exe3");
        data1.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/ex3");
    }

    public  void run() {
        this.spark = SparkSession.builder()
                .appName("Task")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read().parquet(source);
//        df.show(false);
        exe1(df);
        exe2(df);
        exe3(df);
    }

    public static void main(String[] args) {
        Task task = new Task();
        task.run();
    }
}
