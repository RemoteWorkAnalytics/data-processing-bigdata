package processing.features

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import processing.cleaning.SparkCleaning

object FeatureEngineering {

  def main(args: Array[String]): Unit = {

    val mongoUri = "mongodb://admin:password123@mongo:27017"
    val mongoDatabase = "analytics"
    val mongoCollection = "employee_features"

    val spark = SparkSession.builder()
      .appName("Employee Feature Engineering")
      .master("local[*]")
      .config("spark.mongodb.input.uri", mongoUri)
      .config("spark.mongodb.input.database", mongoDatabase)
      .config("spark.mongodb.output.uri", mongoUri)
      .config("spark.mongodb.output.database", mongoDatabase)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-cleaned-stream")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val rawDF = kafkaDF.selectExpr("CAST(value AS STRING) AS value")

    val schema = new StructType()
      .add("employeeId", StringType)
      .add("age", IntegerType)
      .add("industry", StringType)
      .add("yearsOfExperience", IntegerType)
      .add("workLocation", StringType)
      .add("hoursWorkedPerWeek", IntegerType)
      .add("workLifeBalanceRating", StringType)
      .add("stressLevel", StringType)
      .add("mentalHealthCondition", StringType)
      .add("accessToMentalHealthResources", StringType)
      .add("productivityChange", StringType)
      .add("socialIsolationRating", StringType)
      .add("satisfactionWithRemoteWork", StringType)
      .add("companySupportForRemoteWork", StringType)
      .add("physicalActivity", StringType)
      .add("sleepQuality", StringType)
      .add("recordDate", StringType)


    val parsedDF = rawDF
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    val cleanedDF = SparkCleaning.clean(parsedDF)

    val feDF = cleanedDF
      .withColumn("stressProductivityScore", $"productivityChangeInt" - $"stressLevelInt")
      .withColumn(
        "overallWellbeingScore",
        $"stressLevelInt" * -1 +
          $"workLifeBalanceRatingInt" +
          $"socialIsolationRatingInt" * -1 +
          $"physicalActivityInt" +
          $"sleepQualityInt"
      )
      .withColumn(
        "remoteWorkEffectiveness",
        $"satisfactionWithRemoteWorkInt" + $"companySupportForRemoteWorkInt"
      )


    val consoleQuery = feDF.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .start()

    val finalDF = feDF.select(
      $"employeeId",
      $"age",
      $"industry",
      $"yearsOfExperience",
      $"hoursWorkedPerWeek",
      $"recordDate",
      $"stressLevelInt",
      $"productivityChangeInt",
      $"workLocationInt",
      $"physicalActivityInt",
      $"sleepQualityInt",
      $"workLifeBalanceRatingInt",
      $"socialIsolationRatingInt",
      $"satisfactionWithRemoteWorkInt",
      $"companySupportForRemoteWorkInt",
      $"stressProductivityScore",
      $"overallWellbeingScore",
      $"remoteWorkEffectiveness"
    )


    val kafkaFeaturesQuery = feDF
      .select(to_json(struct(col("*"))).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "employee-processed-stream")
      .option("checkpointLocation", "checkpoint/employee-features")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    consoleQuery.awaitTermination()
    kafkaFeaturesQuery.awaitTermination()
  }
}
