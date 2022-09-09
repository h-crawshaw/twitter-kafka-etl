package streamingConsumer

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal
// config-tutorial.scala

object consumer {

  val conf: Config = ConfigFactory.load("resources/application.conf")
  val servers: String = ConfigFactory.load().getString("add.servers")
  val accessKey: String = ConfigFactory.load().getString("add.accessKey")
  val secretAccessKey: String = ConfigFactory.load().getString("add.secretAccessKey")
  val checkpointPath: String = ConfigFactory.load().getString("add.checkpointPath")
  val rawPath: String = ConfigFactory.load().getString("add.rawPath")
  val processedPath: String = ConfigFactory.load().getString("add.processedPath")
  val topics: String = ConfigFactory.load().getString("add.topics")


  val spark: SparkSession = SparkSession.builder()
    .appName("KafkaConsumer")
    .master("local[*]")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("fs.s3a.access.key", accessKey)
    .config("fs.s3a.secret.key", secretAccessKey)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.driver.memory","10G")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.kryoserializer.buffer.max", "2000M")
    .getOrCreate()

  import spark.implicits._

  def readFromKafka(): Unit = {
    val DFschema = StructType(Array(
      StructField("data", StructType(Array(
        StructField("created_at", TimestampType),
        StructField("text", StringType)))),
      StructField("matching_rules", ArrayType(StructType(Array(
        StructField("tag", StringType)
      )))
    )))

    import spark.implicits._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("failOnDataLoss", "false")
      .option("subscribe", topics)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(col("key"), from_json($"value", DFschema).alias("structdata"))
      .select($"key",
        $"structdata.data".getField("created_at").alias("created_at"),
        $"structdata.data".getField("text").alias("text"),
        $"structdata.matching_rules".getField("tag").alias("topic"))
      .withColumn("hour", date_format(col("created_at"), "HH"))
      .withColumn("date", date_format(col("created_at"), "yyyy-MM-dd"))


    kafkaDF
      .writeStream
      .format("parquet") // or console
      .option("checkpointLocation", checkpointPath)
      .option("path", rawPath)
      .outputMode("append")
     // .option("truncate", "false")
      .partitionBy("date", "hour")
      .start()
      .awaitTermination()
  }

  val sentimentPipeline: PretrainedPipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang = "en")

  def transformSentimentDF(sentimentPipeline: PretrainedPipeline): DataFrame = {

    val dateParser: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val hourParser: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")

    def returnCurrentPath: String = {
      val hour: String = LocalDateTime.now().format(hourParser)
      val date: String = if (hour == "00") {
        LocalDateTime.now().minusDays(1).format(dateParser)
      } else {
        LocalDateTime.now().format(dateParser)
      }
      val preHour: String = LocalDateTime.now().minusHours(1).format(hourParser) // minus = 1 change later
      f"${rawPath}date=$date/hour=$preHour/*"
    }

    val paths = returnCurrentPath

    val rawDF: Option[DataFrame] = {
      try {
        Some(spark.read.format("parquet").load(paths))
      } catch {
        case NonFatal(e) =>
          Thread.sleep(3600000)
          val hour = LocalDateTime.now().format(hourParser)
          val date = LocalDateTime.now().format(dateParser)
          val paths = f"${rawPath}date=$date/hour=$hour/*"
          try {
            Some(spark.read.format("parquet").load(paths))
          } catch {
            case NonFatal(e) =>
              println("Path not found.")
              None
          }
      }
    }
    val gotRawDF = rawDF.get

    sentimentPipeline
      .annotate(gotRawDF, "text")
      .select("created_at", "text", "topic")
      .withColumn("sentiment", element_at($"sentiment.result", 1))
  }


  val documentAssembler: DocumentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")
  val tokenizer: Tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")
  val sequenceClassifier: DistilBertForSequenceClassification =
    DistilBertForSequenceClassification.pretrained("distilbert_sequence_classifier_emotion", "en")
    .setInputCols("token", "document")
    .setOutputCol("class")
    .setMaxSentenceLength(512)

  val emotionPipeline: Pipeline = new Pipeline()
    .setStages(Array(documentAssembler, tokenizer, sequenceClassifier))
  def processedDF(sentimentDF: DataFrame, emotionPipeline: Pipeline): DataFrame = {

    emotionPipeline.fit(sentimentDF).transform(sentimentDF)
      .select($"created_at",
        $"text",
        $"topic",
        $"sentiment",
        element_at($"class.result", 1).alias("emotion")
      )
  }

  def aggregateDF(processedDF: DataFrame): Unit = {
    val aggSentiment = processedDF.groupBy("topic")
      .agg(avg(when($"sentiment".eqNullSafe("positive"), 1)
        .otherwise(0)).alias("positivity"),
        count($"topic").alias("counts"))
      .withColumn("created_at", current_timestamp())
      .select($"topic".alias("topic_agg"),
        round($"positivity", 2).alias("positivity_rate"),
        $"counts",
        $"created_at")

    val aggEmotion = processedDF.groupBy("topic", "emotion")
      .agg(count($"topic")).alias("counts")
      .groupBy("topic").pivot("emotion").sum("counts").na.fill(0)

    val innerJoin = aggSentiment.join(aggEmotion,
      aggSentiment.col("topic_agg") === aggEmotion.col("topic"))
      .select("*")

    val mdbUri = "mongodb://<username>:<password>@<clustername>.mongodb.net/<database>.<collection>?retryWrites=true&w=majority"
    innerJoin.write
      .format("mongodb")
      .mode("append")
      .option("uri", mdbUri)
      .save()

    innerJoin.write
      .format("parquet")
      .mode("append")
      .option("path", f"$processedPath")
      .partitionBy("emotion", "counts")
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
    aggregateDF(processedDF(transformSentimentDF(sentimentPipeline), emotionPipeline))
  }
}

