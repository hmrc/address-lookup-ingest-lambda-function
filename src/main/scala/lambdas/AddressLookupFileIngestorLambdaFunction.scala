package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration._
import collection.JavaConverters._

class AddressLookupFileIngestorLambdaFunction extends RequestHandler[jMap[String, String], Unit] {
  override def handleRequest(batch_info: jMap[String, String], context: Context /*Not used*/): Unit = {
    val batchDir = batch_info.get("batchDir")
    val dbSchemaName = batch_info.get("schemaName")
    Await.result(ingestFiles(Repository.forIngest(), dbSchemaName, batchDir), 15.minutes)
  }

  private[lambdas] def ingestFiles(repository: IngestRepository, schemaName: String, processDir: String) = {
    repository.ingestFiles(schemaName, processDir)
  }
}

object AddressLookupFileIngestorLambdaFunction extends App {
  val in = Map("batchDir" -> "", "schemaName" -> "public")
  new AddressLookupFileIngestorLambdaFunction().handleRequest(in.asJava, null)
}