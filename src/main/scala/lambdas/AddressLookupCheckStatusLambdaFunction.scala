package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._

class AddressLookupCheckStatusLambdaFunction extends RequestHandler[String, jMap[String, String]] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupCheckStatusLambdaFunction])

  override def handleRequest(schemaName: String, contextNotUsed: Context): jMap[String, String] = {
    Await.result(checkLookupViewStatus(Repository().forIngest, schemaName), 50.seconds).asJava
  }

  private[lambdas] def checkLookupViewStatus(repository: IngestRepository, schemaName: String): Future[Map[String, String]] = {
    logger.info(s"Checking status of schema $schemaName")

    repository.checkLookupViewStatus(schemaName)
      .map{
        case (status, errorMessage) => Map("status" -> status, "errorMessage" -> errorMessage.orNull)
      }
  }
}
