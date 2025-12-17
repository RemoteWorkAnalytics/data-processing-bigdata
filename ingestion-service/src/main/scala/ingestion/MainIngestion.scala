package ingestion

import ingestion.readers.CsvReader
import ingestion.validators.ValidationRules

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object MainIngestion extends App {
  val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
  rootLogger.setLevel(Level.ERROR)
  val filePath = "ingestion-service/finaldata.csv"
  val topic = "employee-raw-stream"

  val employeeIterator = CsvReader.readEmployeesIterator(filePath)
  val validIterator = employeeIterator.filter(ValidationRules.isValid)

}