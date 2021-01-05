package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration._
import collection.JavaConverters._

class AddressIngestFunction extends RequestHandler[jMap[String, String], Unit] {
  override def handleRequest(batch_info: jMap[String, String], context: Context /*Not used*/): Unit = {
    val batchDir = batch_info.get("batchDir")
    val dbSchemaName = batch_info.get("schemaName")
    Await.result(ingestFiles(dbSchemaName, batchDir), 15.minutes)
  }

  private def ingestFiles(schemaName: String, processDir: String) = {
    val repository = Repository.forIngest()
    repository.ingestFiles(schemaName, processDir)
    //    repository.ingestFile("public.testing", "<root_dir>/testing_copyin.csv")
  }
}

object AddressIngestFunction extends App {
  val in = Map("batchDir" -> "", "schemaName" -> "public")
  new AddressIngestFunction().handleRequest(in.asJava, null)
}