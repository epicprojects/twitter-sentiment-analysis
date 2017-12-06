import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.nio.file.Files
import org.apache.commons.configuration.PropertiesConfiguration


object main {

  val searchTerm = "#iPhoneX"

  val consumerKey = "vjJz6aBx942hfvTfn4YIQ7TQ5"
  val consumerSecret = "APzRR7s5gVOBcX0b5uGDcsKTX1hP5h6hk16JoRKJ8VUuOO0B1q"
  val accessToken = "1896843572-xTLcq2xxchnvKJViyiRbwpXZFM7Xw37OrkjXPWj"
  val accessTokenSecret = "diCyqwQL9HqBuuhtGubd3IpvaEMKZyTYL7irmdGt3arZs"

  val outputFilePath = "/twitter/tweets.parquet"
  val configFile = new File("../application.properties")
  var conf = new PropertiesConfiguration()

  //In case of AzureHD insight replace kafkaBrokers with your cluster kafka-brokers
  val kafkaBrokers = "localhost:9092"
  val schemaRegistryURL = "http://localhost:8081"
  val schemaRegistrySubject = "example.avro.tweet"

  def main(args: Array[String]): Unit = {
    if (!Files.exists(configFile.toPath)) {
      val bw = new PrintWriter(new BufferedWriter(new FileWriter(configFile)))
      bw.println("kafka.brokers = "+kafkaBrokers)
      bw.println("schemaRegistry.url = "+schemaRegistryURL)
      bw.println("schemaRegistry.subject = "+schemaRegistrySubject)
      bw.println("kafka.topics = tweets")
      bw.println("twitter.searchTerms = "+searchTerm)
      bw.println("twitter.oauth.consumerKey = "+consumerKey)
      bw.println("twitter.oauth.consumerSecret = "+consumerSecret)
      bw.println("twitter.oauth.accessToken = "+accessToken)
      bw.println("twitter.oauth.accessTokenSecret = "+accessTokenSecret)
      bw.println("spark.output = "+outputFilePath)
      bw.close()
      conf = new PropertiesConfiguration(configFile.getPath)
    }
    else {
      conf = new PropertiesConfiguration(configFile.getPath)
    }
    new produce().run(conf)
  }
}
