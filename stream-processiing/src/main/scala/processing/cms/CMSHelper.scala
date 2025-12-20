package processing.cms

import com.mongodb.client.{MongoClients, MongoCollection}
import org.apache.spark.util.sketch.CountMinSketch
import org.bson.Document
import org.bson.types.Binary
import java.util.Scanner
import scala.collection.JavaConverters._

object QueryCMSApp {

  private val DB_NAME = "employeeAnalyticsDB"
  private val COLLECTION_NAME = "cmsDashboard"

  // Logic to turn the Binary data from Mongo back into a CMS Object
  private def deserializeCMS(data: Array[Byte]): CountMinSketch = {
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(data))
    val cms = ois.readObject().asInstanceOf[CountMinSketch]
    ois.close()
    cms
  }

  def main(args: Array[String]): Unit = {
    // 1. Connect to MongoDB
    val mongoClient = MongoClients.create("mongodb://localhost:27017")
    val db = mongoClient.getDatabase(DB_NAME)
    val collection: MongoCollection[Document] = db.getCollection(COLLECTION_NAME)

    // 2. Fetch the current dashboard state
    val doc = collection.find(new Document("_id", "dashboard_state")).first()

    if (doc == null) {
      println("No dashboard state found in MongoDB. Make sure the Streaming app has run at least once.")
      return
    }

    // 3. Reconstruct the Sketches
    println("Loading global statistics from database...")
    val stressCMS = deserializeCMS(doc.get("stressCMS").asInstanceOf[Binary].getData)
    val prodCMS   = deserializeCMS(doc.get("prodCMS").asInstanceOf[Binary].getData)
    val workCMS   = deserializeCMS(doc.get("workCMS").asInstanceOf[Binary].getData)

    // 4. Interactive Query Loop
    val scanner = new Scanner(System.in)
    var running = true

    println("\n=== CMS GLOBAL QUERY TOOL ===")
    println("Type a value (e.g., 'High', 'Remote', 'Increased') to see its estimated total count.")
    println("Type 'exit' to quit.")

    while (running) {
      print("\nEnter value to search: ")
      val input = scanner.nextLine().trim

      if (input.toLowerCase == "exit") {
        running = false
      } else if (input.nonEmpty) {
        // Query the value against all three sketches
        val sCount = stressCMS.estimateCount(input)
        val pCount = prodCMS.estimateCount(input)
        val wCount = workCMS.estimateCount(input)

        println(s"Results for '$input':")
        println(s" -> Occurrences in Stress Levels:      $sCount")
        println(s" -> Occurrences in Productivity:      $pCount")
        println(s" -> Occurrences in Work Locations:    $wCount")
      }
    }

    println("Closing query tool.")
    mongoClient.close()
  }
}