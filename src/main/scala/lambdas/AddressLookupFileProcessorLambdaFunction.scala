  package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import processing.Csv

import java.util.{Map => jMap}

class AddressLookupFileProcessorLambdaFunction extends RequestHandler[jMap[String, Object], String] {
  override def handleRequest(batch_info: jMap[String, Object], context: Context /*Not used*/): String = {
    val batchDir = batch_info.get("batchDir").asInstanceOf[String]
    processFiles(batchDir)
    batchDir
  }

  private[lambdas] def processFiles(processDir: String): Unit = {
    new Csv(processDir).process()
  }
}
