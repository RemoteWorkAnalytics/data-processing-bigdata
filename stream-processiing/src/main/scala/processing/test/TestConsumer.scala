package processing.test

import java.util.Properties
import java.util.Arrays
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object TestConsumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test-group")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Arrays.asList("test-topic"))

  println("Waiting for messages...")

  while(true) {
    val records = consumer.poll(java.time.Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(s"Received message: ${record.value()} on partition ${record.partition()}")
    }
  }
}
