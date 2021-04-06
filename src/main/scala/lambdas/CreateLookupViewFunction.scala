package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class CreateLookupViewFunction extends RequestHandler[String, Unit] {
  override def handleRequest(schemaName: String, contextNotUsed: Context): Unit = {
    createLookupView(schemaName)
    Await.ready(createLookupView(schemaName), 30.seconds) //This should be less than the lambda timeout
  }

  private def createLookupView(schemaName: String) = {
    val repository = Repository.forIngest()
    repository.createLookupView(schemaName)
  }
}

object CreateLookupViewFunction extends App {
  new CreateLookupViewFunction().handleRequest("public", null)
}
