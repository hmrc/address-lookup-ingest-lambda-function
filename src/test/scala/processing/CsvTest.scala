package processing

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.Succ

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class CsvTest extends AnyWordSpec with Matchers {
  "Csv" should {
    val rootPath = Paths.get("src", "test", "resources", "csv").toAbsolutePath
    "parse well formatted files" when {
      "2 exist" in {
        val csv = new Csv(rootPath.toString)
        csv.process()

        countLinesInFile("ID10_Header_Records.csv") shouldBe 1
        countLinesInFile("ID11_Street_Records.csv") shouldBe 46694
        countLinesInFile("ID15_StreetDesc_Records.csv") shouldBe 46770
        countLinesInFile("ID21_BLPU_Records.csv") shouldBe 1
        countLinesInFile("ID23_XREF_Records.csv") shouldBe 1
        countLinesInFile("ID24_LPI_Records.csv") shouldBe 1
        countLinesInFile("ID28_DPA_Records.csv") shouldBe 1
        countLinesInFile("ID29_Metadata_Records.csv") shouldBe 1
        countLinesInFile("ID30_Successor_Records.csv") shouldBe 1
        countLinesInFile("ID31_Org_Records.csv") shouldBe 1
        countLinesInFile("ID32_Class_Records.csv") shouldBe 1
        countLinesInFile("ID99_Trailer_Records.csv") shouldBe 1
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
