package ingestion.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import java.text.SimpleDateFormat

object KafkaProducerService extends App {

  val topic = "employee-raw-stream"
  val batchSize = 1000
  val timeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  var batch = List[String]()

  val lines = Source.fromFile(
    "C:/Users/suzan/Downloads/big-data/ingestion-service/finaldata.csv"
  ).getLines().drop(1).toList
  var prevTimestamp: Option[Long] = None

  for (line <- lines) {
    val columns = line.split(",")
    val employeeId = columns(0)
    val timestampStr = columns(21)
    val currentTimestamp = timeFormat.parse(timestampStr).getTime

    prevTimestamp match {
      case Some(prev) =>
        val delay = currentTimestamp - prev
        if (delay > 0) Thread.sleep(delay.min(1000))
      case None =>
    }
    prevTimestamp = Some(currentTimestamp)

    batch = batch :+ line

    if (batch.size >= batchSize) {
      batch.foreach(record => producer.send(new ProducerRecord(topic, record)))
      // Print summary لكل batch
      println(s"Sent batch of ${batch.size} records.")
      batch = List()
    }
  }

  // إرسال أي بيانات متبقية
  if (batch.nonEmpty) {
    batch.foreach(record => producer.send(new ProducerRecord(topic, record)))
    println(s"Sent final batch of ${batch.size} records. Example IDs: ${batch.take(5).map(_.split(",")(0)).mkString(", ")}")
  }

  producer.close()
  println("Simulation finished!")
}