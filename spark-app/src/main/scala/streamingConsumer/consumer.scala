package streamingConsumer

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("fs.s3a.access.key", "AKIAW2FZXUR42DC3HXPU")
    .config("fs.s3a.secret.key", "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")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.driver.memory","10G")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.kryoserializer.buffer.max", "2000M")
    .getOrCreate()

  import spark.implicits._

  def readFromKafka(): Unit = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val DFschema = StructType(Array(
      StructField("data", StructType(Array(
        StructField("created_at", TimestampType),
        StructField("text", StringType)))),
      StructField("matching_rules", ArrayType(StructType(Array(
        StructField("tag", StringType)
      )))
    )))

    val servers = "localhost:29092,localhost:29093,localhost:29094"
    val topics = "biden,putin,zelensky,nato"
    import spark.implicits._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("failOnDataLoss", "false")
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
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
      .option("checkpointLocation", "s3a://twitter-kafka-app/checkpoints/")
      .option("path", "s3a://twitter-kafka-app/raw-data/")
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
      val preHour: String = LocalDateTime.now().minusHours(11).format(hourParser) // minus = 1 change later
      f"s3a://twitter-kafka-app/raw-data/date=$date/hour=$preHour/*"
    }

    //
    //
    val paths = returnCurrentPath

    val rawDF: Option[DataFrame] = {
      try {
        Some(spark.read.format("parquet").load(paths))
      } catch {
        case NonFatal(e) =>
          Thread.sleep(3600000)
          val hour = LocalDateTime.now().format(hourParser)
          val date = LocalDateTime.now().format(dateParser)
          val paths = f"s3a://twitter-kafka-app/raw-data/date=$date/hour=$hour/*"
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
      .option("path", "s3a://twitter-kafka-app/raw-data/")
      .partitionBy("emotion", "counts")
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
    aggregateDF(processedDF(transformSentimentDF(sentimentPipeline), emotionPipeline))
  }
}

