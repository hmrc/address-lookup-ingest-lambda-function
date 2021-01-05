package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class CreateLookupViewFunction extends RequestHandler[String, Unit] {
  override def handleRequest(schemaName: String, contextNotUsed: Context): Unit = {
    createLookupView(schemaName)
    Await.result(createLookupView(schemaName), 5.seconds)
  }

  private def createLookupView(schemaName: String) = {
    val repository = Repository.forIngest()
    repository.createLookupView(schemaName)
  }
}

object CreateLookupViewFunction extends App {
  new CreateLookupViewFunction().handleRequest("public", null)
}
