import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object features {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val users_items = spark.read.load("/user/ildar.ismagilov/users-items/20200429")

    val weblogs = spark.read.option("inferSchema", "true").json("/labs/laba03/weblogs.json")
      .withColumn("tmp", explode(col("visits")))
      .select("uid", "tmp.*")
      .withColumn("timestamp", regexp_replace(
        (col("timestamp").cast(LongType) / 1000).cast(TimestampType).cast(DateType), lit("-"), lit("")))
      .na.drop(Seq("uid"))
      .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .withColumn("url", regexp_replace(col("url"), "[.]", "-"))

    val top1000 = weblogs.groupBy("url").agg(count("*")).orderBy(col("count(1)").desc).na.drop.limit(1000)
      .select("url").orderBy(col("url")).collect.map(_.mkString)

    val vector = weblogs.filter(col("url").isin(top1000: _*))
      .groupBy("uid").pivot("url")
      .agg(count("*")).na.fill(0)
      .withColumn("domain_features", array(top1000.map(x => col(x)): _*))
      .select("uid", "domain_features")

    val days = spark.read.option("inferSchema", "true").json("/labs/laba03/weblogs.json")
      .withColumn("tmp", explode(col("visits")))
      .select("uid", "tmp.timestamp")
      .withColumn("day", date_format(col("timestamp").cast("timestamp"), "EEE"))
      .withColumn("day", concat(lit("web_day_"), lower(col("day"))))
      .groupBy("uid").pivot("day").agg(count("*")).na.fill(0)

    val hours = spark.read.option("inferSchema", "true").json("/labs/laba03/weblogs.json")
      .withColumn("tmp", explode(col("visits")))
      .select("uid", "tmp.timestamp")
      .withColumn("hours", date_format(col("timestamp").cast("timestamp"), "H"))

    val pivotHours = hours.withColumn("hours", concat(lit("web_hour_"), col("hours")))
      .groupBy("uid").pivot("hours").agg(count("*")).na.fill(0)

    val categorized = hours
      .withColumn("cat", when(
        (col("hours") >= 9) && (col("hours") < 18), "web_fraction_work_hours")
        .when((col("hours") >= 18) && (col("hours") <= 23), "web_fraction_evening_hours"))
      .groupBy("uid").pivot("cat").agg(count("*")).drop("null").na.fill(0)


    val result = users_items.join(vector, Seq("uid"), "full")
      .join(days, Seq("uid"), "full")
      .join(pivotHours, Seq("uid"), "full")
      .join(categorized, Seq("uid"), "full")

    result.write.mode("overwrite").save("/user/ildar.ismagilov/features")

    spark.stop()
  }
}