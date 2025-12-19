package processing.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StressTrendsQuery {

  val schema = new StructType()
    .add("employeeId", StringType)
    .add("stressLevelInt", IntegerType)
    .add("recordDate", StringType)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Stress Trends Aggregation")
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
      .groupBy($"recordDate")
      .agg(avg($"stressLevelInt").alias("avgStressLevel"), count("*").alias("employeesCount"))
      .withColumn("lastUpdated", current_timestamp())

    val query = aggDF.writeStream
      .format("mongodb")
      .option("database", "analytics")
      .option("collection", "stress_trends")
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/stress_trends")
      .start()

    query.awaitTermination()
  }
}
