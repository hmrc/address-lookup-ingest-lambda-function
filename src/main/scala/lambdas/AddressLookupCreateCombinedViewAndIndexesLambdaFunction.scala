package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{IngestRepository, Repository}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class AddressLookupCreateCombinedViewAndIndexesLambdaFunction
    extends RequestHandler[String, Unit] {
  private val logger =
    LoggerFactory.getLogger(classOf[AddressLookupCreateCombinedViewAndIndexesLambdaFunction])

  override def handleRequest(schemaName: String,
                             contextNotUsed: Context): Unit = {
    try {
      Await.ready(
        createCombinedViewAndIndexesLambdaFunction(
          Repository().forIngest,
          schemaName
        ),
        10.seconds
      )
    } catch {
      case _: Throwable =>
    }
  }

  private[lambdas] def createCombinedViewAndIndexesLambdaFunction(
    repository: IngestRepository,
    schemaName: String
  ): Future[(Int, Int)] = {
    logger.info(s"Creating combined view and indexes for schema $schemaName")

    repository.createCombinedLookupView(schemaName)
  }
}
