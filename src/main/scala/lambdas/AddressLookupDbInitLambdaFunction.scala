package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import org.slf4j.LoggerFactory
import repositories.{AdminRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

class AddressLookupDbInitLambdaFunction extends RequestHandler[jMap[String, Object], Unit] {
  private val logger = LoggerFactory.getLogger(classOf[AddressLookupDbInitLambdaFunction])

  override def handleRequest(notUsed: jMap[String, Object], contextNotUsed: Context): Unit = {
    Await.result(initialiseDbUsers(Repository().forAdmin), 5.seconds)
  }

  private[lambdas] def initialiseDbUsers(repository: AdminRepository): Future[Unit] = {
    logger.info(s"Initialising database")

    repository.initialiseUsers()
  }
}
