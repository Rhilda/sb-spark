import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.Set

object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate
    spark.sparkContext.getConf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val inputDir = spark.conf.get("spark.users_items.input_dir")
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val update = spark.conf.get("spark.users_items.update")
    val views = spark.read.json(inputDir + "/view")
    val buys = spark.read.json(inputDir + "/buy")
    val logs = views union buys
    val maxDate = "20200430"//logs.select(col("date")).orderBy(col("date").desc).first().mkString
    val exprGetItemName = "regexp_replace(lower(item_id), '-| ', '_')"
    val viewsAgg = views
      .groupBy(col("uid"))
      .pivot(expr("concat('view_', " + exprGetItemName + ")"))
      .count
    val buysAgg = buys
      .groupBy(col("uid"))
      .pivot(expr("concat('buy_', " + exprGetItemName + ")"))
      .count

    var userItems = viewsAgg.join(buysAgg, Seq("uid"), "outer")

    if (update == "1") {
      var userItemsOld = spark.read.parquet(s"$outputDir/20200429")

      val leftItemCols = (Set() ++ userItems.columns) -= "uid"
      val rightItemCols = (Set() ++ userItemsOld.columns) -= "uid"
      val itemCols = leftItemCols ++ rightItemCols

      (itemCols -- leftItemCols).foreach(x => userItems = userItems.withColumn(x, lit(null)))
      (itemCols -- rightItemCols).foreach(x => userItemsOld = userItemsOld.withColumn(x, lit(null)))

      val allItemColsOrdered = itemCols.toSeq.sorted
      userItems = userItems.select((Seq("uid") ++ allItemColsOrdered).map(col): _*)
      userItemsOld = userItemsOld.select((Seq("uid") ++ allItemColsOrdered).map(col): _*)

      userItems = userItems
        .union(userItemsOld)
        .groupBy(col("uid"))
        .agg(lit(0).alias("foo_mock"), allItemColsOrdered.map(x => sum(col(x)).alias(x)): _*)
        .drop(col("foo_mock"))
    }

    val userItemsClean = userItems.na.fill(0, userItems.columns)

    userItemsClean.repartition(200).write.mode("append")
      .parquet(s"$outputDir/$maxDate")

    spark.stop
  }
}