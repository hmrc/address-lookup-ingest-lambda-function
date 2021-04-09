package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration._
import collection.JavaConverters._

class AddressLookupFileIngestorLambdaFunction extends RequestHandler[jMap[String, Object], Unit] {
  override def handleRequest(batch_info: jMap[String, Object], context: Context /*Not used*/): Unit = {
    val batchDir = batch_info.get("batchDir").asInstanceOf[String]
    val dbSchemaName = batch_info.get("schemaName").asInstanceOf[String]
    Await.result(ingestFiles(Repository().forIngest, dbSchemaName, batchDir), 15.minutes)
  }

  private[lambdas] def ingestFiles(repository: IngestRepository, schemaName: String, processDir: String) = {
    repository.ingestFiles(schemaName, processDir)
  }
}
