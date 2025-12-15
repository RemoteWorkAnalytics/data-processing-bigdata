package ingestion.utils

object ConfigLoader {
  // If running locally, use localhost. If in Docker, use "kafka"
  val kafkaBootstrapServers: String = "localhost:9092"

  val rawTopic: String = "employee-raw-stream"

  // Make sure your CSV is actually at this path!
  val csvFilePath: String = """C:\Users\asale\IdeaProjects\data-processing-bigdata\ingestion-service\src\main\resources\remote_work_mental_health.csv"""

  // Simulation settings
  val batchSize: Int = 1000
  val delayBetweenBatchesMs: Long = 2000 // 2 seconds
}