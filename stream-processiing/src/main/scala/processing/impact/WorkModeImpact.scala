package processing.impact

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.Instant

// =======================
// Case classes
// =======================
case class EmployeeRecord(
                           employeeId: String,
                           workLocation: String,
                           stressLevelInt: Option[Int],
                           overallWellbeingScore: Option[Int],
                           remoteWorkEffectiveness: Option[Int],
                           recordDate: String
                         )

case class EmployeeState(
                          workLocation: String,
                          stressLevel: Int,
                          wellbeingScore: Int,
                          remoteEffectiveness: Int
                        )

case class EmployeeImpactEvent(
                                employeeId: String,
                                eventType: String,
                                oldValue: String,
                                newValue: String,
                                impactValue: Int,
                                recordDate: String
                              )

// =======================
// State processor
// =======================
object WorkModeImpactProcessor {

  def process(
               employeeId: String,
               records: Iterator[EmployeeRecord],
               state: GroupState[EmployeeState]
             ): Iterator[EmployeeImpactEvent] = {

    // ⏰ Handle timeout cleanup
    if (state.hasTimedOut) {
      state.remove()
      return Iterator.empty
    }

    if (!records.hasNext) return Iterator.empty
    val record = records.next()

    val prevState = state.getOption.getOrElse(
      EmployeeState(
        record.workLocation,
        record.stressLevelInt.getOrElse(0),
        record.overallWellbeingScore.getOrElse(0),
        record.remoteWorkEffectiveness.getOrElse(0)
      )
    )

    val events = scala.collection.mutable.ListBuffer[EmployeeImpactEvent]()

    // 📍 Work location change
    if (prevState.workLocation != record.workLocation) {
      events += EmployeeImpactEvent(
        employeeId,
        "WORK_LOCATION_CHANGED",
        prevState.workLocation,
        record.workLocation,
        0,
        record.recordDate
      )
    }

    // 😣 Stress change
    record.stressLevelInt.foreach { current =>
      val diff = current - prevState.stressLevel
      if (diff != 0) {
        events += EmployeeImpactEvent(
          employeeId,
          "STRESS_CHANGED",
          prevState.stressLevel.toString,
          current.toString,
          diff,
          record.recordDate
        )
      }
    }

    // 😊 Wellbeing change
    record.overallWellbeingScore.foreach { current =>
      val diff = current - prevState.wellbeingScore
      if (diff != 0) {
        events += EmployeeImpactEvent(
          employeeId,
          "WELLBEING_CHANGED",
          prevState.wellbeingScore.toString,
          current.toString,
          diff,
          record.recordDate
        )
      }
    }

    // 🏠 Remote effectiveness change
    record.remoteWorkEffectiveness.foreach { current =>
      val diff = current - prevState.remoteEffectiveness
      if (diff != 0) {
        events += EmployeeImpactEvent(
          employeeId,
          "REMOTE_EFFECTIVENESS_CHANGED",
          prevState.remoteEffectiveness.toString,
          current.toString,
          diff,
          record.recordDate
        )
      }
    }

    // 🔄 Update state
    state.update(
      EmployeeState(
        record.workLocation,
        record.stressLevelInt.getOrElse(prevState.stressLevel),
        record.overallWellbeingScore.getOrElse(prevState.wellbeingScore),
        record.remoteWorkEffectiveness.getOrElse(prevState.remoteEffectiveness)
      )
    )

    // ⏳ Set timeout
    state.setTimeoutDuration("30 minutes")

    events.iterator
  }
}

// =======================
// Main App
// =======================
object WorkModeImpactApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Employee Work Mode Impact Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-processed-stream")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val employeeSchema = new StructType()
      .add("employeeId", StringType)
      .add("workLocation", StringType)
      .add("stressLevelInt", IntegerType)
      .add("overallWellbeingScore", IntegerType)
      .add("remoteWorkEffectiveness", IntegerType)

    val employeeDS = rawStream
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), employeeSchema).as("data"))
      .filter(col("data").isNotNull)
      .select("data.*")
      .as[(String, String, Int, Int, Int)]
      .map {
        case (id, location, stress, wellbeing, remote) =>
          EmployeeRecord(
            id,
            Option(location).getOrElse("Unknown"),
            Option(stress),
            Option(wellbeing),
            Option(remote),
            Instant.now.toString
          )
      }

    val impactStream = employeeDS
      .groupByKey(_.employeeId)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.ProcessingTimeTimeout()
      )(WorkModeImpactProcessor.process)

    val query = impactStream
      .select(to_json(struct(col("*"))).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "employee-workmode-impact-stream")
      .option("checkpointLocation", "C:/tmp/employee-impact-checkpoint")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    query.awaitTermination()
  }
}
