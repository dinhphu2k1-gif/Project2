package kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class ConsumerLogs {
    private SparkSession spark;

    private static final String destination = "hdfs://m1:8020/project2/result";

    private static final String KAFKA_SERVER = "m1:9092,m2:9092";

    private static final String topic = "sample-data";

    /**
     * Nơi lưu trữ dữ liệu đọc từ Kafka.
     */
    private final String destinationPath = "/project2/data";

    /**
     * Lưu giữ các điểm kiểm tra phục vụ cho việc phục hồi dữ liệu.
     */
    private final String checkpoint = "/tmp/sparkcheckpoint";

    public Dataset<Row> readData() {
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVER)
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")
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

        midDF = midDF.withColumn("year", year(col("timeCreate")))
                .withColumn("month", month(col("timeCreate")))
                .withColumn("day", dayofmonth(col("timeCreate")))
        ;

//        midDF.show();

        return midDF;
    }

    public void writeData() {
        Dataset<Row> df = this.readData();

//        df.write().partitionBy("year", "month", "day").parquet(destinationPath);

        try {
            df.writeStream()
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .format("console")
                    .outputMode("append")
                    .option("truncate", false)
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        try {
            df.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .partitionBy("year", "month", "day")
                    .format("parquet")
                    .option("path", destinationPath + "2")
                    .option("checkpointLocation", checkpoint)
                    .outputMode("append")
                    .start()
                    .awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }



    public void run() {
        this.spark = SparkSession.builder()
                .appName("Phan tich")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        writeData();

//        Dataset<Row> df = readData();
//        exe1(df);
//        exe2(df);
//        exe3(df);
    }

    public static void main(String[] args) {
        ConsumerLogs comsumerLogs = new ConsumerLogs();
        comsumerLogs.run();
    }
}
