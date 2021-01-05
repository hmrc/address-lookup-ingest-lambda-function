package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import java.util.{Map => jMap}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class CheckLookupViewStatusFunction extends RequestHandler[jMap[String, String], Unit] {
  override def handleRequest(notUsed: jMap[String, String], contextNotUsed: Context): Unit = {
    Await.result(initialiseDbUsers(), 5.seconds)
  }

  private def initialiseDbUsers() = {
    val repository = Repository.forAdmin()
    repository.initialiseUsers()
  }
}
