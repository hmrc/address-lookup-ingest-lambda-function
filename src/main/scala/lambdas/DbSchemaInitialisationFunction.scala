package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class DbSchemaInitialisationFunction extends RequestHandler[String, Unit] {
  override def handleRequest(epoch: String, contextNotUsed: Context): Unit = {
    Await.result(initialiseDbSchema(epoch), 5.seconds)
  }

  private def initialiseDbSchema(epoch: String) = {
    val repository = Repository.forAdmin()
    repository.initialiseSchema(epoch)
  }
}

object DbSchemaInitialisationFunction extends App {
  new DbSchemaInitialisationFunction().handleRequest("21", null)
}