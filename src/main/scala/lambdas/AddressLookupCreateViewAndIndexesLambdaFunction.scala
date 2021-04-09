package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AddressLookupCreateViewAndIndexesLambdaFunction extends RequestHandler[String, Unit] {
  override def handleRequest(schemaName: String, contextNotUsed: Context): Unit = {
    Await.result(createLookupView(Repository().forIngest, schemaName), 10.seconds) //This should be less than the lambda timeout
    Thread.sleep(5000)
  }

  private[lambdas] def createLookupView(repository: IngestRepository, schemaName: String) = {
    repository.createLookupView(schemaName)
  }
}
