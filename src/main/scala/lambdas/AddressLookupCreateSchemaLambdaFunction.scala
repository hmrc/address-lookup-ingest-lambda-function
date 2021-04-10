package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{AdminRepository, Repository}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

class AddressLookupCreateSchemaLambdaFunction extends RequestHandler[String, String] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupCreateSchemaLambdaFunction])

  override def handleRequest(epoch: String, contextNotUsed: Context): String = {
    Await.result(initialiseDbSchema(Repository().forAdmin, epoch), 5.seconds)
  }

  private[lambdas] def initialiseDbSchema(repository: AdminRepository, epoch: String): Future[String] = {
    logger.info(s"Creating schema for epoch $epoch")

    repository.initialiseSchema(epoch)
  }
}
