  package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import processing.Csv

import java.util.{Map => jMap}

class AddressFileProcessingFunction extends RequestHandler[jMap[String, String], Unit] {
  override def handleRequest(batch_info: jMap[String, String], context: Context /*Not used*/): Unit = {
    val batchDir = batch_info.get("batchDir")
    processFiles(batchDir)
  }

  private[lambdas] def processFiles(processDir: String): Unit = {
    new Csv(processDir).process()
  }
}
