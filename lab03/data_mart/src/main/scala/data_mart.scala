import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object data_mart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("yarn")
      .appName("laba03")
      .getOrCreate()

    spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    val cass = spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->"labdata", "table"->"clients")).load()
      .withColumn("age_cat",
           when(col("age") > 17 && col("age") < 25, lit("18-24"))
          .when(col("age") > 24 && col("age") < 35, lit("25-34"))
          .when(col("age") > 34 && col("age") < 45, lit("35-44"))
          .when(col("age") > 44 && col("age") < 55, lit("45-54"))
          .otherwise(lit(">=55"))
      ).drop(col("age"))

    val elast = spark.read.format("org.elasticsearch.spark.sql").options(Map("path"->"visits", "pushdown"->"true","es.nodes" -> "10.0.0.31", "es.port" -> "9200")).load.na.drop(Seq("uid"))
      .select(col("uid"), lower(col("category")).as("cat")).withColumn("cat", concat(lit("shop_"), col("cat")))
      .withColumn("cat", regexp_replace(regexp_replace(col("cat"), "-", "_"), " ", "_"))

    val weblogs = spark.read.option("inferSchema", "true").json("/labs/laba03/weblogs.json").na.drop(Seq("uid"))
      .select(col("uid"), explode(col("visits")).as("v")).select("uid", "v.url").withColumn("url", regexp_replace(col("url"), "^https?://(www.)?", ""))
      .withColumn("url", regexp_replace(col("url"), "/.*", ""))

    val webcat = spark.read.format("jdbc").options(Map("url"->"jdbc:postgresql://10.0.0.31:5432/labdata","user"->"ildar_ismagilov", "password"->"KBJZ00mE","driver"->"org.postgresql.Driver", "dbtable"->"domain_cats")).load()
      .withColumn("domain", regexp_replace(col("domain"), "www", "")).withColumn("category", concat(lit("web_"), col("category")))

    val uidomcat = weblogs.join(broadcast(webcat), col("domain") === col("url")).drop("url", "domain").cache

    val uidweb = uidomcat.groupBy("uid").pivot("category").count.cache
    val uidshop = elast.groupBy("uid").pivot("cat").count

    val res = cass.join(uidshop, Seq("uid"), "left").join(uidweb, Seq("uid"), "left").cache

    res.write.format("jdbc").options(Map("url"->"jdbc:postgresql://10.0.0.31:5432/ildar_ismagilov",
      "dbtable"->"clients", "user"->"ildar_ismagilov","password"->"KBJZ00mE", "driver"->"org.postgresql.Driver")).save()
  }
}
