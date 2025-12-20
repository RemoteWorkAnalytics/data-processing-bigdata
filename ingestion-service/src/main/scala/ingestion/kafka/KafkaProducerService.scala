package ingestion.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json
import ingestion.kafka.models.Employee

object KafkaProducerService {

  // We can lower this to 100 for a better "streaming" feel during simulation
  val batchSize = 100

  private def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("acks", "all")
    props.put("retries", "3")
    props.put("linger.ms", "10")
    props.put("batch.size", "32768")
    props.put("compression.type", "snappy")

    new KafkaProducer[String, String](props)
  }

  def sendStream(topic: String, employees: Iterator[Employee]): Unit = {
    val producer = createProducer()

    employees.grouped(batchSize).foreach { batch =>
      batch.foreach { emp =>
        val jsonString = Json.toJson(emp).toString()
        val record =
          new ProducerRecord[String, String](topic, emp.employeeId, jsonString)

        producer.send(record)
        println(s"Sent Employee ID: ${emp.employeeId}")

        // 1. SMALL SLEEP: Wait 100ms between each record
        // This simulates a slow flow of 10 employees per second
        Thread.sleep(100)
      }

      producer.flush()
      println(s"--- Batch of ${batch.size} employees sent to Kafka. ---")

      // 2. BATCH SLEEP: Wait 2 seconds after flushing the batch
      // This gives Spark time to process its 10-second Trigger
      Thread.sleep(2000)
    }

    producer.close()
    println("All employees have been sent to Kafka.")
  }
}