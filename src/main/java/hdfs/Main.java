package hdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    private String sourcePath = "hdfs://m1:8020/sample-data/*";

    private String detinationPath = "hdfs://m1:8020/pageviewlog";

    private String resultPath = "hdfs://m1:8020/project2/result";

    private SparkSession spark;

    /**
     * Thực hiện đọc và ghi dữ liệu vào HDFS
     */
    public void writeParquetFile() {
        System.out.println("START PROCESSING........");
        ReadWrite readWrite = new ReadWrite(this.spark);

        // Đọc dữ liệu
        Dataset<Row> df = readWrite.readData(sourcePath);

        df.show();
        df.printSchema();

        readWrite.writeData(df, detinationPath);
        System.out.println("END PROCESSING.....");
    }

    /**
     * Lấy url đã truy cập nhiều nhất trong ngày của mỗi guid
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
        data3.show();

        // lưu lại kết quả
        data3.write().parquet(resultPath + "/exe1");
    }

    /**
     * Các IP được sử dụng bởi nhiều guid nhất số guid không tính lặp lại
     * @param data
     */
    public void exe2(Dataset<Row> data) {
        Dataset<Row> data1 = data.groupBy("ip").agg(count_distinct(col("guid")).as("count")).orderBy(col("count").desc());
        data1.show();

        data1.write().parquet(resultPath + "/exe2");
    }

    /**
     * Tính các guid mà có timeCreate – cookieCreate nhỏ hơn 30 phút
     * @param data
     */
    public void exe3(Dataset<Row> data) {
        Dataset<Row> data1 = data.select(col("guid")
                , col("timeCreate").cast("long").minus(col("cookieCreate").cast("long")).as("duration"));
        data1 = data1.filter(col("duration").lt(30*60000));
        data1.show();

        data1.write().parquet(resultPath + "/exe3");
    }

    public void run() {
        this.spark = SparkSession.builder().appName("Write file to HDFS").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

//        writeParquetFile();

        Dataset<Row> df = spark.read().parquet(this.detinationPath);
        exe1(df);
        exe2(df);
        exe3(df);
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }
}
