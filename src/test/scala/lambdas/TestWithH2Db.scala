package lambdas

import lambdas.DbSchemaInitialisationFunction
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import repositories.{AdminRepository, IngestRepository, Repository}

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._

class TestWithH2Db extends AnyWordSpec with Matchers {
  val timeout = 5.seconds
//  val adminRepository = new AdminRepository(Repository.testH2Transactor)
//  val ingestRepository = new IngestRepository(Repository.testH2Transactor)
  val adminRepository = Repository.forAdmin()
  val ingestRepository = Repository.forIngest()

  "???" should {
    val epoch = 10
    var schemaName = "NOT_SET"
    "create schema" when {
      "initialiseDbSchema is executed" in {
        val epoch = 10
        schemaName = Await.result(new DbSchemaInitialisationFunction().initialiseDbSchema(adminRepository, "10"), timeout)
        schemaName should startWith(s"ab${epoch}_")
      }
    }

    "create users" when {
      "initialiseDbUsers is executed" in {
        Await.result(new DbUserInitialisationFunction().initialiseDbUsers(adminRepository), timeout)
      }
    }

    "ingest data" when {
      "ingest is executed" in {
        val res = Await.result(new AddressIngestFunction().ingestFiles(ingestRepository, schemaName,
          new File("src/test/resources/csv/").getAbsolutePath), timeout)
      }
    }

    "create lookup view" when {
      "createLookupView is executed" in {
        val res = Await.result(new CreateLookupViewFunction().createLookupView(ingestRepository, schemaName), timeout)
      }
    }
  }
}
