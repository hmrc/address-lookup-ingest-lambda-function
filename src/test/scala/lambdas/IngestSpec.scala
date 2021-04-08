package lambdas

import doobie.implicits._
import lambdas.AddressLookupCreateSchemaLambdaFunction
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import repositories.{AdminRepository, IngestRepository, Repository}

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._

class IngestSpec extends AsyncWordSpec with Matchers {
  val timeout = 5.seconds
  val adminRepository = Repository.forAdmin()
  val ingestRepository = Repository.forIngest()

  "Ingestion process" should {
    val epoch = "10"
    var schemaName = "NOT_SET"

    "create schema" when {
      "initialiseDbSchema is executed" in {
        (for {
          schemaName <- new AddressLookupCreateSchemaLambdaFunction().initialiseDbSchema(adminRepository, epoch)
          schemas <- adminRepository.listSchemas
        } yield (schemaName, schemas))
          .map {
            case (schema, schemas) =>
              schemaName = schema
              schema should startWith(s"ab${epoch}_")
              schemas should contain(schemaName)
          }
      }
    }

    "create users" when {
      "initialiseDbUsers is executed" in {
        (for {
          _ <- new AddressLookupDbInitLambdaFunction().initialiseDbUsers(adminRepository)
          users <- adminRepository.listUsers
        } yield users)
          .map(users => users should contain allOf("addresslookupingestor", "addresslookupreader"))
      }
    }

    "ingest data" when {
      "ingest is executed" in {
        new AddressLookupFileIngestorLambdaFunction().ingestFiles(ingestRepository, schemaName,
          new File("src/test/resources/csv/").getAbsolutePath)
                                   .map(inserted => inserted should be > 0)
      }
    }

    "create lookup view" when {
      "createLookupView is executed - the data is probably not correct here as there are no rows - TODO" in {
        for {
          _ <- new AddressLookupCreateViewAndIndexesLambdaFunction().createLookupView(ingestRepository, schemaName)
          created <- ingestRepository.checkIfLookupViewCreated(schemaName)
        } yield created shouldBe true
      }
    }

    "check lookup view was created" when {
      "checkLookupViewStatus is executed" in {
        for {
          resultMap <- new lambdas.AddressLookupCheckStatusLambdaFunction().checkLookupViewStatus(ingestRepository, schemaName)
        } yield {
          resultMap("status") shouldBe "completed"
          resultMap("errorMessage") shouldBe null
        }
      }
    }

    "finalise schema" when {
      "finaliseSchema is executed" in {
        for {
          finalised <- new AddressLookupFinaliseSchemaLambdaFunction().finaliseSchema(ingestRepository, epoch, schemaName)
        } yield finalised shouldBe true
      }
    }
  }
}
