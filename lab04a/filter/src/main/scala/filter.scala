import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_json, lit, regexp_replace, schema_of_json}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object filter {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder.getOrCreate()
//        .master(conf.get("spark.filter.master", "yarn")).getOrCreate()
      spark.sparkContext.getConf.set("spark.sql.session.timeZone", "UTC")
      val offsetr = spark.conf.get("spark.filter.offset", "earliest")
      val topic = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
      val offset: String = Try(offsetr.toInt)
      match {
      	case Success(v) => s"""{\"${topic}\":{\"0\":${v}}}"""
      	case Failure(_) => offsetr
      }
      val dir = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix", "visits")
      val kafkaOptions = Map("kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667", "subscribe" -> topic,
       "startingOffsets" -> offset)

      val kafka = spark.read.format("kafka").options(kafkaOptions).load
      import spark.implicits._
      val schema = schema_of_json(lit(kafka.select(col("value")).as[String].first))

      val board = kafka.select($"value".cast("string")).withColumn("val",
        from_json(col("value"), schema, Map[String, String]().asJava)).select("val.*")

      val buy = board.filter(col("event_type") === "buy")
        .withColumn("date", regexp_replace((col("timestamp").cast(LongType) / 1000).cast(TimestampType).cast(DateType), lit("-"), lit("")))
        .withColumn("ddate", col("date"))

      val view = board.filter(col("event_type") === "view")
        .withColumn("date", regexp_replace((col("timestamp").cast(LongType) / 1000).cast(TimestampType).cast(DateType), lit("-"), lit("")))
        .withColumn("ddate", col("date"))

      view.write.mode("overwrite").partitionBy("ddate").json(s"$dir/view")

      buy.write.mode("overwrite").partitionBy("ddate").json(s"$dir/buy")

  }
}
