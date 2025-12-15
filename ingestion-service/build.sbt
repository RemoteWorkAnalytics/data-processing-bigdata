name := "ingestion-service"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // Kafka Client
  "org.apache.kafka" % "kafka-clients" % "3.6.0",
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.2.13",
  // CSV Reader
  "com.github.tototoshi" %% "scala-csv" % "1.3.10"
)