package lambdas

import org.scalatest.Ignore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import processing.Csv
import repositories.Repository

import java.io.File
import java.nio.file.Paths
import scala.concurrent.duration._
import scala.io.Source

//@Ignore
class IngestSpec extends AsyncWordSpec with Matchers {
  private val timeout = 5.seconds

  private def repositories: Repository.Repositories = Repository.forTesting()

  private def adminRepository = repositories.forAdmin

  private def ingestRepository = repositories.forIngest

  "Ingestion process" should {
    val epoch = "10"
    var schemaName = "NOT_SET"

    "process csv files" when {
      val rootPath = Paths.get("src", "test", "resources", "csv").toAbsolutePath

      "Csv.process executed for 2 existing files" in {
        val csv = new Csv(rootPath.toString)
        csv.process()

        countLinesInFile("ID10_Header_Records.csv") shouldBe 2
        countLinesInFile("ID11_Street_Records.csv") shouldBe 46788
        countLinesInFile("ID15_StreetDesc_Records.csv") shouldBe 46788
        countLinesInFile("ID21_BLPU_Records.csv") shouldBe 1
        countLinesInFile("ID23_XREF_Records.csv") shouldBe 1
        countLinesInFile("ID24_LPI_Records.csv") shouldBe 1
        countLinesInFile("ID28_DPA_Records.csv") shouldBe 1
        countLinesInFile("ID29_Metadata_Records.csv") shouldBe 2
        countLinesInFile("ID30_Successor_Records.csv") shouldBe 1
        countLinesInFile("ID31_Org_Records.csv") shouldBe 1
        countLinesInFile("ID32_Class_Records.csv") shouldBe 1
        countLinesInFile("ID99_Trailer_Records.csv") shouldBe 2
      }
    }

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
      "createLookupView is executed" in {
        for {
          _ <- new AddressLookupCreateViewAndIndexesLambdaFunction().createLookupView(ingestRepository, schemaName)
          created <- ingestRepository.checkIfLookupViewCreated(schemaName)
        } yield created shouldBe true
      }
    }

    "check lookup view was created" when {
      "checkLookupViewStatus is executed" in {
        for {
          resultMap <- new lambdas.AddressLookupCheckStatusLambdaFunction().checkLookupViewStatus(ingestRepository,
            schemaName)
        } yield {
          resultMap("status") shouldBe "completed"
          resultMap("errorMessage") shouldBe null
        }
      }
    }

    "finalise schema" when {
      "finaliseSchema is executed" in {
        for {
          finalised <- new AddressLookupFinaliseSchemaLambdaFunction().finaliseSchema(ingestRepository, epoch,
            schemaName)
        } yield finalised shouldBe true
      }
    }
  }

  private def countLinesInFile(fileName: String) = {
    val basePath = "src/test/resources/csv/"
    val source = Source.fromFile(new File(basePath + fileName))
    val lines = source.getLines().length
    source.close()
    lines
  }
}
