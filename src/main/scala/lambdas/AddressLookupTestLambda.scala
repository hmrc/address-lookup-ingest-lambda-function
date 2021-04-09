package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.jessecoyle.JCredStash
import repositories.Repository

import java.util.{Map => jMap}
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AddressLookupTestLambda extends RequestHandler[jMap[String, Object], Unit] {
  override def handleRequest(notUsed: jMap[String, Object], contextNotUsed: Context): Unit = {
    doTest()
  }

  private def doTest(): Unit = {
    val context = Map("role" -> "address_lookup_file_download").asJava
    val host = new JCredStash().getSecret("address_lookup_rds_host", context)
    println(s">>> host: $host")
  }
}
