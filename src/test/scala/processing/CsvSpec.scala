package processing

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar

import java.io.File
import scala.io.Source

class CsvSpec extends AnyWordSpec with Matchers with MockitoSugar {
  "Csv" ignore {
    "process all csv files" when {
      "process called" in {
        val csv = new Csv("src/test/resources/csv/")
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

    def countLinesInFile(fileName: String) = {
      val basePath = "src/test/resources/csv/"
      val source = Source.fromFile(new File(basePath + fileName))
      val lines = source.getLines().length
      source.close()
      lines
    }
  }
}
