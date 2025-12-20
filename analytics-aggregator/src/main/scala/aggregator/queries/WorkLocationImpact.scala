package processing.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WorkLocationImpact {

  val schema = new StructType()
    .add("employeeId", StringType)
    .add("workLocation", StringType)
    .add("overallWellbeingScore", DoubleType)
    .add("remoteWorkEffectiveness", DoubleType)
    .add("stressProductivityScore", DoubleType)
    .add("stressLevelInt", DoubleType)
    .add("productivityChangeInt", DoubleType)
    .add("recordDate", StringType)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WorkLocation Impact Aggregation")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
      .getOrCreate()

    import spark.implicits._


    val sourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-processed-stream")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .withColumn("recordDate", to_date($"recordDate"))


    val aggDF = sourceDF
      .groupBy($"recordDate", $"workLocation")
      .agg(
        count("*").alias("totalEmployees"),
        round(avg($"overallWellbeingScore"), 3).alias("overallWellbeing"),
        round(avg($"remoteWorkEffectiveness"), 3).alias("remoteEffectiveness"),
        round(avg($"stressProductivityScore"), 1).alias("productivityScore"),
        round(avg($"productivityChangeInt"), 1).alias("avgProductivity"),
        round(avg($"stressLevelInt"), 1).alias("avgStress")
      )
      .withColumn("lastUpdated", current_timestamp())

    val query = aggDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()



    val query1 = aggDF.writeStream
      .format("mongodb")
      .option("database", "analytics")
      .option("collection", "worklocation_impact")
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/worklocation_impact")
      .start()

    query.awaitTermination()
    query1.awaitTermination()
  }
}
