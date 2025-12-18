package processing.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import processing.cleaning.SparkCleaning
import org.apache.spark.sql.streaming.Trigger

object RawKafkaConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Raw Kafka Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-raw-stream")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val rawDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    val employeeSchema = new StructType()
      .add("employeeId", StringType)
      .add("age", IntegerType)
      .add("gender", StringType)
      .add("jobRole", StringType)
      .add("industry", StringType)
      .add("yearsOfExperience", IntegerType)
      .add("workLocation", StringType)
      .add("hoursWorkedPerWeek", IntegerType)
      .add("numberOfVirtualMeetings", IntegerType)
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
      .add("region", StringType)
      .add("variantIndex", IntegerType)
      .add("recordDate", StringType)
      .add("generatedNote", StringType)

    val df = rawDF
      .select(from_json($"value", employeeSchema).as("data"))
      .select("data.*")

    val cleanedDF = SparkCleaning.clean(df)
      .filter($"employeeId".isNotNull && $"age" > 0 && $"yearsOfExperience" >= 0 && $"hoursWorkedPerWeek" >= 0)

    val query = cleanedDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        println(s"Processing batch $batchId with ${batchDF.count()} records")

        batchDF.collect().foreach { row =>
          println(s"Employee ID: ${row.getAs[String]("employeeId")}, Age: ${row.getAs[Int]("age")}, Stress: ${row.getAs[String]("stressLevel")}")
        }

        batchDF
          .select(to_json(struct($"*")).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "employee-cleaned-stream")
          .save()
      }
      .option("checkpointLocation", "checkpoints/employee-cleaned")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()


  }
}