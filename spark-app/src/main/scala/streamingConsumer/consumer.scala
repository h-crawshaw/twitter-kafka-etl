package streamingConsumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object consumer {

  val spark: SparkSession = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[*]")
    // .config("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("fs.s3a.access.key", "AKIAW2FZXUR42DC3HXPU")
    .config("fs.s3a.secret.key", "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")
    .getOrCreate()


  def readFromKafka(): Unit = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val DFschema = StructType(Array(
      StructField("data", StructType(Array(
        StructField("created_at", TimestampType),
        StructField("text", StringType)))
      )))
    val servers = "localhost:29092,localhost:29093,localhost:29094"
    import spark.implicits._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("failOnDataLoss", "false")
      .option("subscribe", "twitter-housing")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(col("key"), from_json($"value", DFschema).alias("structdata"))
      .select($"key",
        $"structdata.data".getField("created_at").alias("created_at"),
        $"structdata.data".getField("text").alias("text"))
      .withColumn("hour", date_format(col("created_at"), "HH"))
      .withColumn("date", date_format(col("created_at"), "yyyy-MM-dd"))

    kafkaDF
      .writeStream
      .format("parquet") // or console
      .option("checkpointLocation", "s3a://twitter-kafka-app/checkpoints/")
      .option("path", "s3a://twitter-kafka-app/processed-data/")
      //   .outputMode("append")
      .option("truncate", "false")
      .partitionBy("date", "hour")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
  }



}
