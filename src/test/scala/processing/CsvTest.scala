package processing

import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Paths

class CsvTest extends AnyWordSpec {
  "Csv" should {
    val rootPath = Paths.get("src", "test", "resources", "csv").toAbsolutePath
    "parse well formatted files" when {
      "2 exist" in {
        val csv = new Csv(rootPath.toString)
        csv.process()
      }
    }
  }

}
