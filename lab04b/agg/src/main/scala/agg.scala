import org.apache.spark.sql.streaming.Trigger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_json, lit, schema_of_json, struct, sum, to_json, when, window}
import org.apache.spark.sql.types._

object agg {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.getConf.set("spark.sql.session.timeZone", "UTC")

    val stream_kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers","spark-master-1:6667")
      .option("subscribe","ildar_ismagilov").load()

    val schema2 = StructType(Seq(StructField("category", StringType, true), StructField("event_type", StringType, true),
      StructField("item_id", StringType, true), StructField("item_price", StringType, true),
      StructField("timestamp", LongType, true), StructField("uid", StringType, true)))

    val stream = stream_kafka.select(col("value").cast("string")).withColumn("val", from_json(col("value"), schema2)).select("val.*")

    val mart = stream.withColumn("date", (col("timestamp").cast(LongType)/1000).cast(TimestampType))

    val res = mart.groupBy(window(col("date"), "1 hours")).agg(
      sum(when(col("event_type") === "buy", col("item_price"))).as("revenue"),
      sum(when(col("uid").isNotNull, 1)).as("visitors"),
      sum(when(col("event_type") === "buy", 1)).as("purchases")
    ).withColumn("aov", col("revenue") / col("purchases"))
      .withColumn("start_ts", col("window.start").cast("long"))
      .withColumn("end_ts", col("window.end").cast("long"))
      .drop(col("window"))

    val go = res.select(col("start_ts").cast("string").as("key"), to_json(struct("*")).as("value"))
      .writeStream.trigger(Trigger.ProcessingTime("5 seconds")).format("kafka")
      .option("checkpointLocation", "/tmp/ch").option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "ildar_ismagilov_lab04b_out").option("maxOffsetsPerTrigger", 300)
      .outputMode("update")
      .start()

    go.awaitTermination()

  }
}
