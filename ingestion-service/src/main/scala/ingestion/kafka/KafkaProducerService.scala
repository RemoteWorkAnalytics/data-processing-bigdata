package ingestion.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json
import ingestion.kafka.models.Employee

object KafkaProducerService {

  val batchSize = 500

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
      }

      producer.flush()
      println(s"Batch of ${batch.size} employees sent to Kafka.")
    }

    producer.close()
    println("All employees have been sent to Kafka.")
  }
}
