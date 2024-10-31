package processing

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.Try

trait FileOps {
  def cleanOldFiles(fileName: String): Unit = {
    if (Files.exists(Paths.get(fileName))) {
      val filesAndFoldersToDelete = Files.walk(Paths.get(fileName)).iterator().asScala.toList
        .groupBy(_.toFile.isFile)
      filesAndFoldersToDelete.get(true).foreach(
        _.foreach { file =>
          Files.deleteIfExists(file)
        })
      filesAndFoldersToDelete.get(false).foreach(
        _.sortBy(_.getNameCount).reverse
          .foreach { folder =>
            Try(Files.deleteIfExists(folder))
          })
    }
  }
}
