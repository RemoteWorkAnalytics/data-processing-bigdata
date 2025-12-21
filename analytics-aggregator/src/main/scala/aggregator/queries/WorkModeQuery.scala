package processing.queries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WorkModeQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WorkModeQuery")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrap = "localhost:9092"
    val topic = "employee-processed-stream"
    val checkpointDir = "checkpoints/work_mode"

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val jsonDF = rawDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json",
        schema = new org.apache.spark.sql.types.StructType()
          .add("employeeId", "string")
          .add("recordDate", "string")
          .add("workLocation", "string")
      ).as("data"))
      .select("data.*")

    val aggregatedDF = jsonDF.groupBy($"recordDate", $"workLocation")
      .agg(count("employeeId").alias("employeeCount"))

    val query = aggregatedDF.writeStream
      .format("mongodb")
      .option("checkpointLocation", checkpointDir)
      .option("spark.mongodb.output.database", "analytics")
      .option("spark.mongodb.output.collection", "work_mode")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
