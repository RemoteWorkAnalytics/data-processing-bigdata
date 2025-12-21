package processing.insights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object BurnoutAlertEngine {

  private val MONGO_URI = "mongodb://localhost:27017"
  private val DB_NAME = "analytics"
  private val COLLECTION_NAME = "dept_burnout_alertss"
  private val CHECKPOINT_PATH = "checkpoints/burnout_state_trackeer"
  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  private val INPUT_TOPIC = "employee-processed-stream"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Departmental State Tracker")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", MONGO_URI)
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val schema = new StructType()
      .add("employeeId", StringType)
      .add("jobRole", StringType)
      .add("stressLevelInt", IntegerType)


    val kafkaRawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", INPUT_TOPIC)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val processedDF = kafkaRawDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
      .filter($"jobRole".isNotNull && $"jobRole" =!= "null")

    val departmentalState = processedDF
      .groupBy($"jobRole".as("department"))
      .agg(
        avg($"stressLevelInt").as("avgStress"),
        count($"employeeId").as("employeeCount")
      )
      .filter($"avgStress" > 1.5)
      .withColumn("lastUpdated", current_timestamp())
      .withColumn("recommendation", when($"department" === "Software Engineer", "Optimize Sprint Velocity")
        .when($"department" === "Data Scientist", "Protect Focus Hours")
        .when($"department" === "Sales", "Wellness Break Required")
        .otherwise("Mental Health Check-in Needed"))


    val query = departmentalState.writeStream
      .outputMode("complete")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {


          val upsertDF = batchDF.withColumn("_id", $"department")

          upsertDF.write
            .format("mongodb")
            .option("database", DB_NAME)
            .option("collection", COLLECTION_NAME)
            .mode("append")
            .save()

          println(s" State Updated for ${batchDF.count()} departments at ${java.time.LocalTime.now()}")
          upsertDF.show(false)
        }
      }
      .option("checkpointLocation", CHECKPOINT_PATH)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}