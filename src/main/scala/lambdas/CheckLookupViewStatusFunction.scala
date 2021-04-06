package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._

class CheckLookupViewStatusFunction extends RequestHandler[String, jMap[String, String]] {
  override def handleRequest(schemaName: String, contextNotUsed: Context): jMap[String, String] = {
    Await.result(checkLookupViewStatus(Repository.forIngest(), schemaName), 5.seconds).asJava
  }

  private def checkLookupViewStatus(repository: IngestRepository, schemaName: String): Future[Map[String, String]] = {
    repository.checkLookupViewStatus(schemaName)
      .map{case (status, errorMessage) => Map("status" -> status, "errorMessage" -> errorMessage)}
  }
}

object CheckLookupViewStatusFunction extends App {
  new CheckLookupViewStatusFunction().handleRequest(null, null)
}