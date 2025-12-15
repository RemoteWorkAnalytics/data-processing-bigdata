package ingestion.utils

import org.slf4j.LoggerFactory

trait Logging {

  protected val logger: org.slf4j.Logger = LoggerFactory.getLogger(this.getClass)
}