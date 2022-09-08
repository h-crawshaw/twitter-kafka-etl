package streamingConsumer

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal
// config-tutorial.scala

object consumer {

//  val config = ConfigFactory.load("resources/application.properties")
//  val servers = ConfigFactory.load().getString("confs.servers")
//  val accessKey = ConfigFactory.load().getString("confs.accessKey")
//  val secretAccessKey = ConfigFactory.load().getString("confs.secretAccessKey")
//  val checkpointPath = ConfigFactory.load().getString("confs.checkpointPath")
//  val rawPath = ConfigFactory.load().getString("confs.rawPath")
//  val processedPath = ConfigFactory.load().getString("confs.processedPath")

  val spark: SparkSession = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[*]")
    // .config("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("fs.s3a.access.key", "AKIAW2FZXUR42DC3HXPU")
    .config("fs.s3a.secret.key", "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")
    .getOrCreate()

  import spark.implicits._

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
      .outputMode("append")
      //   .option("truncate", "false")
      .partitionBy("date", "hour")
      .start()
      .awaitTermination()
  }

  def transformSentimentDF: DataFrame = {

    val dateParser: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val hourParser: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")

    def returnCurrentPath: String = {
      val hour: String = LocalDateTime.now().format(hourParser)
      val date: String = if (hour == "00") {
        LocalDateTime.now().minusDays(1).format(dateParser)
      } else {
        LocalDateTime.now().format(dateParser)
      }
      val preHour: String = LocalDateTime.now().minusHours(1).format(hourParser)
      f"s3a://twitter-kafka-app/raw-data/date=$date/hour=$preHour"
    }


    val paths = returnCurrentPath

    //      val hour = LocalDateTime.now().format(hourParser)
    //      val date = LocalDateTime.now().format(dateParser)

    val rawDF: DataFrame = {
      try {
        spark.read.format("parquet").load(paths)
      } catch {
        case NonFatal(e) =>
        Thread.sleep(3600000)
        val hour = LocalDateTime.now().format(hourParser)
        val date = LocalDateTime.now().format(dateParser)
        val paths = f"s3a://twitter-kafka-app/raw-data/date=$date/hour=$hour/*"
        try {
          spark.read.format("parquet").load(paths)
        } catch {
          case NonFatal(e) => None
          print("No path found.")
          }
        }
      null
    }

    val sentiment_pipeline =PretrainedPipeline("analyze_sentimentdl_use_twitter", lang="en")
    sentiment_pipeline
      .annotate(rawDF, "text")
      .select("created_at", "text")
      .withColumn("sentiment", element_at($"sentiment.result", 1))
  }

//  val documentAssembler = new DocumentAssembler()
//    .setInputCol("text")
//    .setOutputCol("document")
//  val tokenizer = new Tokenizer()
//    .setInputCols("document")
//    .setOutputCol("token")
//  val sequenceClassifier = DistilBertForSequenceClassification.pretrained("distilbert_sequence_classifier_emotion", "en")
//    .setInputCols("token", "document")
//    .setOutputCol("class")
//    .setMaxSentenceLength(512)

//  def processDF(sentimentDF: DataFrame): DataFrame = {
//    val emotion_pipeline = new Pipeline()
//      .setStages(Array(documentAssembler, tokenizer, sequenceClassifier))
//
//    emotion_pipeline.fit(sentimentDF).transform(sentimentDF)
//      .select($"created_at",
//        $"text",
//        $"sentiment",
//        element_at($"class.result", 1).alias("emotion")
//      )
//  }

//  def aggregateDF(processedDF: DataFrame): Unit = {
//    val aggSentiment = processedDF.groupBy("topic")
//      .agg(avg(when($"sentiment".eqNullSafe("positive"), 1)
//        .otherwise(0)).alias("positivity"),
//        count($"topic").alias("counts"))
//      .withColumn("created_at", current_timestamp())
//      .select($"topic".alias("topic_agg"),
//        round($"positivity", 2).alias("positivity_rate"),
//        $"counts",
//        $"created_at")
//
//    val aggEmotion = processedDF.groupBy("topic", "emotion")
//      .agg(count($"topic")).alias("counts")
//      .groupBy("topic").pivot("emotion").sum("counts").na.fill(0)
//
//    val innerJoin = aggSentiment.join(aggEmotion,
//      aggSentiment.col("topic_agg") === aggEmotion.col("topic"))
//      .select("*")
//
//    innerJoin.write.mode("append").csv("src/main/coolcsvbro.csv")
//  }

  def main(args: Array[String]): Unit = {
    // readFromKafka()
    transformSentimentDF
  }
}

