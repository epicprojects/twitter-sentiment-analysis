import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.io.serializer.avro.AvroRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

class produce {

  def run(conf: Configuration) = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("kafka.brokers"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", conf.getString("schemaRegistry.url"))

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
   .setOAuthConsumerKey(conf.getString("twitter.oauth.consumerKey"))
   .setOAuthConsumerSecret(conf.getString("twitter.oauth.consumerSecret"))
   .setOAuthAccessToken(conf.getString("twitter.oauth.accessToken"))
   .setOAuthAccessTokenSecret(conf.getString("twitter.oauth.accessTokenSecret"))

    val schemaRegistry = new CachedSchemaRegistryClient(conf.getString("schemaRegistry.url"), 1000)
    if (!schemaRegistry.getAllSubjects.contains(conf.getString("schemaSubject.subject"))) {
      Console.println("Registering schema")
      val schema = new Schema.Parser().parse(getClass.getResourceAsStream("/avro/twitter.avsc"))
      schemaRegistry.register(conf.getString("schemaRegistry.subject"), schema)

    }
    val m = schemaRegistry.getLatestSchemaMetadata(conf.getString("schemaRegistry.subject"))
    val schemaId = m.getId
    val schema = schemaRegistry.getById(schemaId)

    val producer = new KafkaProducer[Any, GenericRecord](props)

    // Converts a twitter4j Status object to and AvroRecord
    def toTweet(status: Status): GenericRecord = {
    val tweet = new GenericData().newRecord(new AvroRecord (), schema).asInstanceOf[GenericRecord]
      tweet.put("id", status.getId)
      tweet.put("lang", status.getLang)
      tweet.put("createdAt", status.getCreatedAt.toInstant.toEpochMilli)
      tweet.put("retweetCount", status.getRetweetCount)
      tweet.put("text", status.getText)
      tweet.put("geoLocation",status.getGeoLocation)
      tweet.put("location", Option(status.getUser.getLocation).getOrElse(""))
      tweet
    }

    // Listens to new tweets from twitter4j api data stream and sends them over kafka
    val listener = new StatusAdapter() {
      override def onStatus(status: Status): Unit = {
        System.out.println("Sending to Kafka")
        producer.send(new ProducerRecord(conf.getString("kafka.topics"), toTweet(status)))
        System.out.println("Sent to Kafka")
      }

      override def onException(ex: Exception): Unit = {
        ex.printStackTrace()
      }
    }
    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance
    twitterStream.addListener(listener)

    //Query the stream based on your defined search term
    val query = new FilterQuery
    query.track(conf.getString("twitter.searchTerms"))
    twitterStream.filter(query)
  }

}
