package mongo

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.mongodb.spark.sql._

object MongoConnection {



  val mongoUri: String = "mongodb://admin:password123@mongo:27017/analytics.employee_features"

  def createSparkSession(appName: String = "Spark Mongo App"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.mongodb.output.uri", mongoUri)
      .config("spark.mongodb.input.uri", mongoUri)
      .getOrCreate()
  }

  def writeToMongo(df: DataFrame, collection: String = "employee_features", mode: String = "append"): Unit = {
    df.write
      .format("mongo")
      .option("uri", mongoUri)
      .option("collection", collection)
      .mode(mode)
      .save()
  }

  def readFromMongo(spark: SparkSession, collection: String = "employee_features"): DataFrame = {
    spark.read
      .format("mongo")
      .option("uri", mongoUri)
      .option("collection", collection)
      .load()
  }
}
