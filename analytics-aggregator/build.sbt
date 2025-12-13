name := "analytics-aggregator"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.10.0",
  "com.typesafe.play" %% "play-json" % "2.10.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11"
)
