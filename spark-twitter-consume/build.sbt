
name := "spark-twitter-consume"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Confluent" at "http://packages.confluent.io/maven"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-sql" % sparkVersion,
"org.apache.spark" %% "spark-core" % sparkVersion,
"org.apache.spark" %% "spark-streaming" % sparkVersion,
"org.apache.spark" %% "spark-mllib" % sparkVersion,
"org.apache.spark" %% "spark-hive" % sparkVersion,
"edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" ,
"edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models"
)

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "commons-configuration" % "commons-configuration" % "1.6"
libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.3.0" exclude("com.fasterxml.jackson.core", "jackson-databind")
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"



assemblyMergeStrategy in assembly := {
                case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
                case default => MergeStrategy.first
}
