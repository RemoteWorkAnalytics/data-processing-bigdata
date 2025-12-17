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

    df
      .withColumn("stressLevelInt", mapColumn("stressLevel", stressMap))
      .withColumn("productivityChangeInt", mapColumn("productivityChange", productivityMap))
      .withColumn("workLocationInt", mapColumn("workLocation", workLocationMap))
      .withColumn("physicalActivityInt", mapColumn("physicalActivity", physicalActivityMap))
      .withColumn("sleepQualityInt", mapColumn("sleepQuality", sleepQualityMap))
      .withColumn("workLifeBalanceRatingInt", col("workLifeBalanceRating").cast("int"))
      .withColumn("socialIsolationRatingInt", col("socialIsolationRating").cast("int"))
      .withColumn("satisfactionWithRemoteWorkInt", col("satisfactionWithRemoteWork").cast("int"))
      .withColumn("companySupportForRemoteWorkInt", col("companySupportForRemoteWork").cast("int"))
  }
}
