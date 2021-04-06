package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class CreateLookupViewFunction extends RequestHandler[String, Unit] {
  override def handleRequest(schemaName: String, contextNotUsed: Context): Unit = {
    Await.ready(createLookupView(Repository.forIngest(), schemaName), 30.seconds) //This should be less than the lambda timeout
  }

  private[lambdas] def createLookupView(repository: IngestRepository, schemaName: String) = {
    repository.createLookupView(schemaName)
  }
}

object CreateLookupViewFunction extends App {
  new CreateLookupViewFunction().handleRequest("public", null)
}
