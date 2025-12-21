name := "ingestion-service"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.6.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "com.typesafe.play" %% "play-json" % "2.10.0",
"com.github.tototoshi" %% "scala-csv" % "1.3.10"
)
