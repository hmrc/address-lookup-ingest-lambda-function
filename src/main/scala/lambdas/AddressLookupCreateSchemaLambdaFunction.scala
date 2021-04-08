package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{Repository, AdminRepository}

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AddressLookupCreateSchemaLambdaFunction extends RequestHandler[String, String] {
  override def handleRequest(epoch: String, contextNotUsed: Context): String = {
    Await.result(initialiseDbSchema(Repository.forAdmin(), epoch), 5.seconds)
  }

  private[lambdas] def initialiseDbSchema(repository: AdminRepository, epoch: String) = {
    repository.initialiseSchema(epoch)
  }
}
