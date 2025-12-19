package processing.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ImpactAggregationQuery {

  // =======================
  // Schema of impact events
  // =======================
  val impactSchema = new StructType()
    .add("employeeId", StringType)
    .add("eventType", StringType)
    .add("oldValue", StringType)
    .add("newValue", StringType)
    .add("impactValue", IntegerType)
    .add("recordDate", StringType)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Employee Impact Aggregation")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // =======================
    // Read from Kafka
    // =======================
    val sourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-workmode-impact-stream")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", impactSchema).as("data"))
      .select("data.*")
      .withColumn("eventTimestamp", to_timestamp($"recordDate"))
      .withColumn("eventDate", to_date($"eventTimestamp"))

    // =======================
    // Aggregation
    // =======================
    val aggDF = sourceDF
      .groupBy($"eventDate", $"eventType")
      .agg(
        sum($"impactValue").alias("totalImpact"),
        avg($"impactValue").alias("avgImpact"),
        count("*").alias("eventsCount")
      )
      .withColumn("lastUpdated", current_timestamp())

    // =======================
    // Write to MongoDB
    // =======================
    val query = aggDF.writeStream
      .format("mongodb")
      .option("database", "analytics")
      .option("collection", "impact_trends")
      .outputMode("complete")
      .option("checkpointLocation", "checkpoints/impact_trends")
      .start()

    query.awaitTermination()
  }
}
