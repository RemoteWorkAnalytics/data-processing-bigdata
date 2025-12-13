name := "rest-api"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.10.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11"
)
