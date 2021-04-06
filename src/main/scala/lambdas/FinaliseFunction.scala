package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.Repository

import java.util.{Map => jMap}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class FinaliseFunction extends RequestHandler[jMap[String, String], Boolean] {
  override def handleRequest(epochData: jMap[String, String], contextNotUsed: Context): Boolean = {
    val epoch = epochData.get("epoch")
    val schemaName = epochData.get("schemaName")

    println(s"Finalising epoch: $epoch schema_name: $schemaName")

    Await.result(finaliseSchema(epoch, schemaName), 5.seconds)
  }

  private def finaliseSchema(epoch: String, schemaName: String): Future[Boolean] = {
    val repository = Repository.forIngest()
    repository.finaliseSchema(epoch, schemaName)
  }
}

object FinaliseFunction extends App {
  new FinaliseFunction().handleRequest(null, null)
}