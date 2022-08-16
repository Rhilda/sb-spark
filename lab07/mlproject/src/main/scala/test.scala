import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.{Pipeline, PipelineModel}

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate

    val modelPath = spark.conf.get("spark.mlproject.model_path", "/user/ildar.ismagilov/laba07/model/")
    val kafkaInputTopic = spark.conf.get("spark.mlproject.kafka_input_topic", "ildar_ismagilov")
    val kafkaOutputTopic = spark.conf.get("spark.mlproject.kafka_output_topic", "ildar_ismagilov_lab07_out")
    val kafkaServer = "spark-master-1:6667"

    val model = PipelineModel.load(modelPath)

    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
        StructField("url", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true))))
        , nullable = true)
    ))

    val exprGetDomain = "regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"

    val logs = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("subscribe", kafkaInputTopic).load

    val kafkaSink = logs.writeStream.trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val preparedLogs = batchDF
          .select(col("timestamp"), from_json(col("value").cast("string"), schema).as("valueParsed"))
          .select(col("valueParsed.*"))
          .select(col("uid"), explode(col("visits")).as("visits"))
          .withColumn("domain", expr(exprGetDomain))
          .groupBy(col("uid"))
          .agg(collect_list("domain").as("domains"))
        val result = model.transform(preparedLogs)
        result.select(lit(null).cast(StringType),
          to_json(struct(col("uid"), col("label_string").as("gender_age"))).as("value"))
          .write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", kafkaOutputTopic).save
      }

    kafkaSink.start
  }
}