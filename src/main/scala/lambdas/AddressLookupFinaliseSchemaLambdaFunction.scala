package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import repositories.{IngestRepository, Repository}

import java.util.{Map => jMap}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class AddressLookupFinaliseSchemaLambdaFunction extends RequestHandler[jMap[String, Object], Boolean] {
  override def handleRequest(epochData: jMap[String, Object], contextNotUsed: Context): Boolean = {
    val epoch = epochData.get("epoch").asInstanceOf[String]
    val schemaName = epochData.get("schemaName").asInstanceOf[String]

    println(s"Finalising epoch: $epoch schema_name: $schemaName")

    Await.result(finaliseSchema(Repository.forIngest(), epoch, schemaName), 5.seconds)
  }

  private[lambdas] def finaliseSchema(repository: IngestRepository, epoch: String, schemaName: String): Future[Boolean] = {
    repository.finaliseSchema(epoch, schemaName)
  }
}
