import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.io.serializer.avro.AvroRecord
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import twitter4j._

class consume {

  def run(conf: Configuration) = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("kafka.avro.consumer")
      .getOrCreate()

    val schemaRegistry = new CachedSchemaRegistryClient(conf.getString("schemaRegistry.url"), 1000)
    val m = schemaRegistry.getLatestSchemaMetadata(conf.getString("schemaRegistry.subject"))
    val schemaId = m.getId
    val schema = schemaRegistry.getById(schemaId)

    // Kafka configuration
    // The Kafka topic(s) to read from
    val topics = Array(conf.getString("kafka.topics"))
    // Batching interval when reading
    val batchInterval = 2

    // A function that creates a streaming context
    def createStreamingContext(): StreamingContext = {

      spark.sparkContext.setLogLevel("ERROR")
      // Create a new StreamingContext from the default context.
      val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval))

      // Kafka parameters when reading
      // auto.offset.reset = 'earliest' reads from the beginning of the queue
      //     Set to 'latest' to only receive new messages as they are added to the queue.
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> conf.getString("kafka.brokers"),
        "key.deserializer" -> classOf[KafkaAvroDeserializer],
        "value.deserializer" -> classOf[KafkaAvroDeserializer],
        "group.id" -> "test1",
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "schema.registry.url" -> conf.getString("schemaRegistry.url")
      )

      // Create the stream from Kafka
      val messageStream = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, GenericRecord](topics, kafkaParams)
      )

      // Get only the tweets (in deserialized Avro format)
      val tweetsAvro = messageStream.map(record => record.value)

      // Convert the records to dataframes, so we can select interesting values
      tweetsAvro.foreachRDD {
        rdd =>
          // because sometimes there's not really an RDD there
          if (rdd.count() >= 1) {
            val tweetObj = rdd.map(
              v => {
                Row.fromSeq(List[Any](
                  v.get("id"),
                  v.get("createdAt"),
                  v.get("lang").toString, // Type is org.apache.avro.util.Utf8
                  v.get("retweetCount"),
                  v.get("text").toString,
                  v.get("location").toString
                ))
              })


            val stopword_path = "NLTK_English_Stopwords_Corpus.txt"
            val stopWordsList = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(stopword_path))

            val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

            val filtered = tweetObj.filter(t => t(2).asInstanceOf[String] == "en")


            val preprocessed = filtered.map(row => {

              val tweet = row.toSeq.toList
              val tweetText = tweet(4).asInstanceOf[String].replaceAll("#","")
              val urlPattern = Pattern.compile("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?")
              val matcher = urlPattern.matcher(tweetText)
              var tweetWithoutHashtagAndUrl = ""
              if (matcher.find) {
                tweetWithoutHashtagAndUrl = matcher.replaceAll("")
              }
              else {
                tweetWithoutHashtagAndUrl = tweetText
              }
              Row.fromSeq(tweet ++ List(tweetWithoutHashtagAndUrl))
            })




            val classifed = preprocessed.map(row => {
                 val tweet = row.toSeq.toList
                 val tweetText = tweet(6).asInstanceOf[String]
                 val tweetLang = tweet(2).asInstanceOf[String]
                 var nlpScore = 0
                 val cleanedTweetText = tweetText.replaceAll("\n", "")
                 nlpScore = CoreNLPSentimentAnalyzer.computeWeightedSentiment(cleanedTweetText)
                 Row.fromSeq(tweet ++ List(nlpScore))
            })



            val schema = StructType(
              StructField("id", LongType, false) ::
                StructField("createdAt", LongType, false) ::
                StructField("lang", StringType, false) ::
                StructField("retweetCount", IntegerType, false) ::
                StructField("Orignaltext", StringType, false) ::
                StructField("location", StringType, false) ::
                StructField("Trimmedtext", StringType, false) ::
                StructField("NLP", IntegerType, false) :: Nil)


            val tweetRaw = spark.createDataFrame(classifed, schema)
            val tweetInfo = tweetRaw
              .withColumn("createdAt", from_unixtime(col("createdAt").divide(1000)))
              .withColumn("year", year(col("createdAt")))
              .withColumn("month", month(col("createdAt")))
              .withColumn("day", dayofmonth(col("createdAt")))


            // Show 5 in the console
            // val count = tweetInfo.count()
            // print("\nTWEETS: "+count.toString()+"\n")
            //tweetInfo.show(3)



            /*
           // Append to Parquet
           tweetInfo
             .write
             .partitionBy("year", "month", "day")
             .mode(SaveMode.Append)
             .save("/home/osboxes/Desktop/Output/") */


            // or alternatively  write your results to a csv file
          val outputfile = "/home/osboxes/Desktop/results"
          var filename = "myinsights"
          var outputFileName = outputfile + "/temp_" + filename
          var mergedFileName = outputfile + "/merged_" + filename
          var mergeFindGlob = outputFileName

          tweetInfo.write
            .format("com.databricks.spark.csv")
            .option("header", "false")
              .option("delimiter","\t")
            .mode(SaveMode.Overwrite)
            .save(outputFileName)

          merge(mergeFindGlob, mergedFileName)
          tweetInfo.unpersist()


          }
      }


      // Tell the stream to keep the data around for a minute, so it's there when we query later
      ssc.remember(Minutes(1))
      // Checkpoint for fault-tolerance
      // ssc.checkpoint("/tweetcheckpoint")
      // Return the StreamingContext
      ssc
    }

    // Stop any existing StreamingContext
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach {
        _.stop(stopSparkContext = false)
      }
    }

    // Get or create a StreamingContext
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)

    // This starts the StreamingContext in the background.
    ssc.start()

    // Set the stream to run with a timeout of batchInterval * 60 * 1000 seconds
    // If you don't set the time it will keep running forever
    //ssc.awaitTerminationOrTimeout(batchInterval * 60 * 1000)
    ssc.awaitTermination()
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    if(hdfs.exists(new Path(srcPath)))
      {
        print("src:exsist")
        FileUtil.chmod(srcPath,"ugo+rw",true)
      }
    if(hdfs.exists(new Path(dstPath)))
    {
      print("dst:exsist")
      FileUtil.chmod(dstPath,"ugo+rw")

    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    // the "true" setting deletes the source files once they are merged into the new output
  }

}
