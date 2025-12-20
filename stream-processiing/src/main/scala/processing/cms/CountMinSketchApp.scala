package processing.cms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.CountMinSketch
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.mongodb.client.model.ReplaceOptions
import org.bson.Document
import org.bson.types.Binary
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._

object CountMinSketchApp {

  private lazy val mongoClient: MongoClient = MongoClients.create("mongodb://127.0.0.1:27017")
  private val DB_NAME = "analytics"
  private val COLLECTION_NAME = "cmsDashboard"

  private val EPSILON = 0.001
  private val CONFIDENCE = 0.99
  private val SEED = 42

  private def serializeCMS(cms: CountMinSketch): Array[Byte] = {
    val baos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(baos)
    oos.writeObject(cms)
    oos.close()
    baos.toByteArray
  }

  private def deserializeCMS(data: Array[Byte]): CountMinSketch = {
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(data))
    val cms = ois.readObject().asInstanceOf[CountMinSketch]
    ois.close()
    cms
  }

  private def writeToMongo(cmsMap: Map[String, CountMinSketch]): Unit = {
    val db = mongoClient.getDatabase(DB_NAME)
    val collection: MongoCollection[Document] = db.getCollection(COLLECTION_NAME)

    val stateDoc = new Document("_id", "dashboard_state")
    cmsMap.foreach { case (key, cms) =>
      stateDoc.append(key, new Binary(serializeCMS(cms)))
    }
    collection.replaceOne(new Document("_id", "dashboard_state"), stateDoc, new ReplaceOptions().upsert(true))

    val summaryDoc = new Document("_id", "live_counts")
      .append("stress_high", cmsMap("stressCMS").estimateCount("high"))
      .append("stress_medium", cmsMap("stressCMS").estimateCount("medium"))
      .append("stress_low", cmsMap("stressCMS").estimateCount("low"))
      .append("prod_increased", cmsMap("prodCMS").estimateCount("increase"))
      .append("prod_decreased", cmsMap("prodCMS").estimateCount("decrease"))
      .append("prod_stayed_same", cmsMap("prodCMS").estimateCount("no change"))
      .append("work_remote", cmsMap("workCMS").estimateCount("remote"))
      .append("work_onsite", cmsMap("workCMS").estimateCount("on-site") + cmsMap("workCMS").estimateCount("onsite"))
      .append("work_hybrid", cmsMap("workCMS").estimateCount("hybrid"))
      .append("lastUpdated", new java.util.Date().toString)

    collection.replaceOne(new Document("_id", "live_counts"), summaryDoc, new ReplaceOptions().upsert(true))
    println(s"🟢 Updated: Stress High=${cmsMap("stressCMS").estimateCount("high")}, Prod Incr=${cmsMap("prodCMS").estimateCount("increase")}")
  }

  private def readFromMongo(): Map[String, CountMinSketch] = {
    val db = mongoClient.getDatabase(DB_NAME)
    val collection = db.getCollection(COLLECTION_NAME)
    val doc = collection.find(new Document("_id", "dashboard_state")).first()

    if (doc == null) Map.empty[String, CountMinSketch]
    else {
      doc.keySet().asScala.filter(_ != "_id").map { key =>
        key -> deserializeCMS(doc.get(key).asInstanceOf[Binary].getData)
      }.toMap
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMS Productivity Tracker")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employee-cleaned-stream")
      .option("startingOffsets", "earliest")
      .load()

    val employeeSchema = new org.apache.spark.sql.types.StructType()
      .add("stressLevel", "string")
      .add("productivityChange", "string")
      .add("workLocation", "string")

    val df = kafkaDF.selectExpr("CAST(value AS STRING) as json_payload")
      .select(from_json($"json_payload", employeeSchema).as("data"))
      .select("data.*")
      .filter($"stressLevel".isNotNull && $"productivityChange".isNotNull && $"workLocation".isNotNull)
      .withColumn("stressLevel", lower(trim($"stressLevel")))
      .withColumn("productivityChange", lower(trim($"productivityChange")))
      .withColumn("workLocation", lower(trim($"workLocation")))

    val query = df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        if (batchDF.count() > 0) {
          val prevCMSMap = readFromMongo()
          val globalStressCMS = prevCMSMap.getOrElse("stressCMS", CountMinSketch.create(EPSILON, CONFIDENCE, SEED))
          val globalProdCMS   = prevCMSMap.getOrElse("prodCMS",   CountMinSketch.create(EPSILON, CONFIDENCE, SEED))
          val globalWorkCMS   = prevCMSMap.getOrElse("workCMS",   CountMinSketch.create(EPSILON, CONFIDENCE, SEED))

          globalStressCMS.mergeInPlace(batchDF.stat.countMinSketch("stressLevel", EPSILON, CONFIDENCE, SEED))
          globalProdCMS.mergeInPlace(batchDF.stat.countMinSketch("productivityChange", EPSILON, CONFIDENCE, SEED))
          globalWorkCMS.mergeInPlace(batchDF.stat.countMinSketch("workLocation", EPSILON, CONFIDENCE, SEED))

          writeToMongo(Map("stressCMS" -> globalStressCMS, "prodCMS" -> globalProdCMS, "workCMS" -> globalWorkCMS))
        }
        batchDF.unpersist()
        ()
      }
      .option("checkpointLocation", "checkpoints/cms-dashboard")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}