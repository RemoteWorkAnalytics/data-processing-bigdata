package processing.cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkCleaning {

  def clean(df: DataFrame): DataFrame = {

    val stressMap = Map("Low" -> 1, "Medium" -> 2, "High" -> 3)
    val productivityMap = Map("Decrease" -> -1, "No Change" -> 0, "Increase" -> 1)
    val workLocationMap = Map("Onsite" -> 1, "Hybrid" -> 2, "Remote" -> 3)
    val physicalActivityMap = Map("None" -> 0, "Weekly" -> 1, "Daily" -> 2)
    val sleepQualityMap = Map("Poor" -> 1, "Average" -> 2, "Good" -> 3)

    def mapColumn(colName: String, mapping: Map[String, Int]) =
      mapping.foldLeft(lit(0)) { case (colExpr, (key, value)) =>
        when(df(colName) === key, value).otherwise(colExpr)
      }

    val cleanedDF = df

      .withColumn("stressLevelInt", when(col("stressLevel").isNotNull, mapColumn("stressLevel", stressMap)).otherwise(lit(0)))
      .withColumn("productivityChangeInt", when(col("productivityChange").isNotNull, mapColumn("productivityChange", productivityMap)).otherwise(lit(0)))
      .withColumn("workLocationInt", when(col("workLocation").isNotNull, mapColumn("workLocation", workLocationMap)).otherwise(lit(0)))
      .withColumn("physicalActivityInt", when(col("physicalActivity").isNotNull, mapColumn("physicalActivity", physicalActivityMap)).otherwise(lit(0)))
      .withColumn("sleepQualityInt", when(col("sleepQuality").isNotNull, mapColumn("sleepQuality", sleepQualityMap)).otherwise(lit(0)))

      // cast للأعمدة الرقمية
      .withColumn("workLifeBalanceRatingInt", col("workLifeBalanceRating").cast("int"))
      .withColumn("socialIsolationRatingInt", col("socialIsolationRating").cast("int"))
      .withColumn("satisfactionWithRemoteWorkInt", col("satisfactionWithRemoteWork").cast("int"))
      .withColumn("companySupportForRemoteWorkInt", col("companySupportForRemoteWork").cast("int"))

      .na.fill(Map(
        "workLifeBalanceRatingInt" -> 0,
        "socialIsolationRatingInt" -> 0,
        "satisfactionWithRemoteWorkInt" -> 0,
        "companySupportForRemoteWorkInt" -> 0
      ))


      .filter(
        col("employeeId").isNotNull &&
          col("age").isNotNull &&
          col("yearsOfExperience").isNotNull &&
          col("hoursWorkedPerWeek").isNotNull
      )

    cleanedDF
  }
}