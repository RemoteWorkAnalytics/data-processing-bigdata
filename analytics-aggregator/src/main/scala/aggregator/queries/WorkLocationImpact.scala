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
    .add("accessToMentalHealthResources", StringType)

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


    val aggDF = sourceDF
      .groupBy($"workLocation")
      .agg(
        count("*").alias("totalEmployees"),

        round(avg($"overallWellbeingScore"), 3).alias("overallWellbeing"),

        round(avg($"remoteWorkEffectiveness"), 3).alias("remoteEffectiveness"),

        round(avg($"stressProductivityScore"), 1).alias("productivityScore"),

        round(avg($"productivityChangeInt"), 1).alias("avgProductivity"),

        round(avg($"stressLevelInt"), 1).alias("avgStress"),

        sum(when($"accessToMentalHealthResources" === "Yes", 1).otherwise(0)).alias("employeesWithAccess"),
        round(
          sum(when($"accessToMentalHealthResources" === "Yes", 1).otherwise(0)) / count("*") * 100,
          2
        ).alias("percentWithAccess"),

        round(avg(when($"accessToMentalHealthResources" === "Yes", $"stressLevelInt")), 2).alias("avgStressWithAccess"),
        round(avg(when($"accessToMentalHealthResources" === "No", $"stressLevelInt")), 2).alias("avgStressWithoutAccess"),

        round(avg(when($"accessToMentalHealthResources" === "Yes", $"productivityChangeInt")), 2).alias("avgProductivityWithAccess"),
        round(avg(when($"accessToMentalHealthResources" === "No", $"productivityChangeInt")), 2).alias("avgProductivityWithoutAccess")
      )
      .withColumn("lastUpdated", current_timestamp())


    val query1 = aggDF.writeStream
      .format("mongodb")
      .option("database", "analytics")
      .option("collection", "worklocation_impact")
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/worklocation--impact")
      .start()

    query1.awaitTermination()
  }
}
