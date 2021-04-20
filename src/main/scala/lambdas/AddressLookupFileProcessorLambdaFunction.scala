  package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import processing.Csv

import java.util.{Map => jMap}

class AddressLookupFileProcessorLambdaFunction extends RequestHandler[jMap[String, Object], String] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupFinaliseSchemaLambdaFunction])

  override def handleRequest(batchInfo: jMap[String, Object], contextNotUsed: Context): String = {
    val batchDir = batchInfo.get("batchDir").asInstanceOf[String]

    processFiles(batchDir)
    batchDir
  }

  private[lambdas] def processFiles(processDir: String): Unit = {
    logger.info(s"Processing address files in dir $processDir")

    new Csv(processDir).process()
  }
}
