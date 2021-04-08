package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class AddressLookupCreateSchemaLambdaFunction extends RequestHandler[String, String] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupCreateSchemaLambdaFunction])

  override def handleRequest(epoch: String, contextNotUsed: Context): String = {
    Await.result(initialiseDbSchema(Repository().forIngest, epoch), 5.seconds)
  }

  private[lambdas] def initialiseDbSchema(repository: IngestRepository, epoch: String): Future[String] = {
    logger.info(s"Creating schema for epoch $epoch")

    repository.initialiseSchema(epoch)
  }
}
