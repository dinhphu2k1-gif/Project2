package kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.to_timestamp;

public class ConsumerLogs {
    private SparkSession spark;

    private final String destination = "hdfs://m1:8020/data";

    public static final String KAFKA_SERVER = "m1:9092,m2:9092";

    public static final String topic = "sample-data";

    public Dataset<Row> readData() {
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVER)
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");

        df = df.select(split(col("value"), "\t").as("split"));

//        df.show(false);

        Dataset<Row> midDF = df.select(to_timestamp(col("split").getItem(0)).as("timeCreate")
                , to_timestamp(col("split").getItem(1)).as("cookieCreate")
                , col("split").getItem(2).cast("int").as("browserCode")
                , col("split").getItem(3).as("browserVer")
                , col("split").getItem(4).cast("int").as("osCode")
                , col("split").getItem(5).as("osVer")
                , col("split").getItem(6).cast("long").as("ip")
                , col("split").getItem(7).cast("int").as("locId")
                , col("split").getItem(8).as("domain")
                , col("split").getItem(9).cast("int").as("siteId")
                , col("split").getItem(10).cast("int").as("cId")
                , col("split").getItem(11).as("path")
                , col("split").getItem(12).as("referer")
                , col("split").getItem(13).cast("long").as("guid")
                , col("split").getItem(14).as("flashVersion")
                , col("split").getItem(15).as("jre")
                , col("split").getItem(16).as("sr")
                , col("split").getItem(17).as("sc")
                , col("split").getItem(18).cast("int").as("geographic")
                , col("split").getItem(23).as("category"));

        midDF.show();

        return midDF;
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
//        data3.write().parquet(resultPath + "/exe1");
    }

    /**
     * Các IP được sử dụng bởi nhiều guid nhất số guid không tính lặp lại
     * @param data
     */
    public void exe2(Dataset<Row> data) {
        Dataset<Row> data1 = data.groupBy("ip").agg(count_distinct(col("guid")).as("count")).orderBy(col("count").desc());
        data1.show();

//        data1.write().parquet(resultPath + "/exe2");
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

//        data1.write().parquet(resultPath + "/exe3");
    }

    public void run() {
        this.spark = SparkSession.builder()
                .appName("Upload data to HDFS")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = readData();


    }

    public static void main(String[] args) {
        ConsumerLogs comsumerLogs = new ConsumerLogs();
        comsumerLogs.run();
    }
}
