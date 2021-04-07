  package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import processing.Csv

import java.util.{Map => jMap}

class AddressFileProcessingFunction extends RequestHandler[jMap[String, String], String] {
  override def handleRequest(batch_info: jMap[String, String], context: Context /*Not used*/): String = {
    val batchDir = batch_info.get("batchDir")
    processFiles(batchDir)
    batchDir
  }

  private[lambdas] def processFiles(processDir: String): Unit = {
    new Csv(processDir).process()
  }
}
