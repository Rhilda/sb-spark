import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_json, lit, schema_of_json}
import org.apache.spark.sql.types.{LongType, TimestampType}

import collection.JavaConverters._

object filter {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder.master("local[1]").getOrCreate()
//        .master(conf.get("spark.filter.master", "yarn")).getOrCreate()
      spark.sparkContext.getConf.set("spark.sql.session.timeZone", "UTC")
      val offset = spark.sparkContext.getConf.get("spark.filter.offset", "0").toInt
      val dir = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix", "visits")

      val kafka = spark.read.format("kafka").option("kafka.bootstrap.servers", "spark-master-1.newprolab.com:6667")
        .option("subscribe", "lab04_input_data").option("startingOffsets", s"""{ "lab04_input_data": {"0": $offset} }""")
        .load()
      import spark.implicits._
      val schema = schema_of_json(lit(kafka.select(col("value")).as[String].first))

      val board = kafka.select($"value".cast("string")).withColumn("val",
        from_json(col("value"), schema, Map[String, String]().asJava)).select("val.*")

      val buy = board.filter(col("event_type") === "buy")
        .withColumn("date", date_format((col("timestamp").cast(LongType)/1000).cast(TimestampType),"YYYYMMDD"))
        .withColumn("ddate", col("date").cast("string"))

      val view = board.filter(col("event_type") === "view")
        .withColumn("date", date_format((col("timestamp").cast(LongType)/1000).cast(TimestampType),"YYYYMMDD"))
        .withColumn("ddate", col("date").cast("string"))

      view.write.mode("overwrite").partitionBy("ddate").json(s"$dir/view")

      buy.write.mode("overwrite").partitionBy("ddate").json(s"$dir/buy")

  }
}
