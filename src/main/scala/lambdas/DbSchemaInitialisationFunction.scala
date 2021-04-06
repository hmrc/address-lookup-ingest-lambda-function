package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{Repository, AdminRepository}

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class DbSchemaInitialisationFunction extends RequestHandler[String, Unit] {
  override def handleRequest(epoch: String, contextNotUsed: Context): Unit = {
    Await.result(initialiseDbSchema(Repository.forAdmin(), epoch), 5.seconds)
  }

  private[lambdas] def initialiseDbSchema(repository: AdminRepository, epoch: String) = {
    repository.initialiseSchema(epoch)
  }
}

object DbSchemaInitialisationFunction extends App {
  new DbSchemaInitialisationFunction().handleRequest(args(0), null)
}