package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class AddressLookupFileIngestorLambdaFunction extends RequestHandler[jMap[String, Object], Unit] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupFinaliseSchemaLambdaFunction])

  override def handleRequest(batchInfo: jMap[String, Object], contextNotUsed: Context): Unit = {
    val batchDir = batchInfo.get("batchDir").asInstanceOf[String]
    val dbSchemaName = batchInfo.get("schemaName").asInstanceOf[String]

    Await.result(ingestFiles(Repository().forIngest, dbSchemaName, batchDir), 15.minutes)
  }

  private[lambdas] def ingestFiles(repository: IngestRepository, schemaName: String, processDir: String): Future[Int] = {
    logger.info(s"Ingesting files for schema $schemaName from dir $processDir")

    repository.ingestFiles(schemaName, processDir)
  }
}
