package hdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.to_timestamp;

public class ReadWrite {
    private SparkSession spark;

    public ReadWrite(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Đọc dữ liệu và chuyển đổi về DataFrame
     * @param path : đường dẫn tới dữ liệu
     * @return
     */
    public Dataset<Row> readData(String path) {
        Dataset<Row> df = this.spark.read().text(path);
        df = df.withColumn("split", split(col("value"), "\t"));

        Dataset<Row> resdf = df.select(to_timestamp(col("split").getItem(0)).as("timeCreate")
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

        return resdf;
    }

    /**
     * Ghi dữ liệu vào HDFS
     * @param df
     * @param path
     */
    public void writeData(Dataset<Row> df, String path) {
        df.write().parquet(path);
    }
}
