package processing

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipInputStream
import scala.jdk.CollectionConverters.*

class FileProcessor() extends FileOps {

  def process(downloadedFiles: List[String], batchesRootDir: String): Map[File, List[Path]] = {
    cleanOldFiles(batchesRootDir)
    downloadedFiles.map(f => unzipFileTo(f, batchesRootDir))
    batchFiles(batchesRootDir)
  }

  private def batchFiles(dataRootDir: String): Map[File, List[Path]] = {
    import implicits.*

    Files.walk(Paths.get(dataRootDir), 1)
      .filter(_.toFile.isFile)
      .filter(_.toFile.getName.endsWith(".csv"))
      .sorted()
      .iterator.asScala.toList
      .grouped(20)
      .toList.zipWithIndex
      .flatMap {
        case (files, idx) =>
          files.map { f =>
            val batchDir = Paths.get(dataRootDir, s"$idx").toFile.ensureDirsExist()
            val batchFilePath = Paths.get(batchDir.getAbsolutePath, f.toFile.getName)
            Files.move(
              Paths.get(dataRootDir, f.toFile.getName),
              batchFilePath,
              java.nio.file.StandardCopyOption.ATOMIC_MOVE)
            batchDir -> batchFilePath
          }
      }.groupBy(x => x._1)
      .view
      .mapValues(x => x.map(_._2))
      .toMap
  }

  private def unzipFileTo(fileName: String, outputDir: String): (File, List[File]) = {
    import implicits.*
    val zin = new ZipInputStream(new FileInputStream(new File(fileName)))
    val outputDirectory = new File(outputDir).ensureDirsExist()

    val result = LazyList.continually(zin.getNextEntry).takeWhile(_ != null).map { file =>
      val foutf = new File(outputDirectory, file.getName)
      val fout = new FileOutputStream(foutf)
      val buffer = new Array[Byte](1024)
      LazyList
        .continually(zin.read(buffer))
        .takeWhile(_ != -1)
        .foreach(fout.write(buffer, 0, _))
      foutf
    }.toList
    outputDirectory -> result
  }

  def filesInBatch(batchRootDir: String): List[String] = {
    Files.walk(Paths.get(batchRootDir))
      .iterator().asScala
      .filter(_.toFile.isFile)
      .map(p => p.toFile.getAbsolutePath).toList
  }
}

object FileProcessor {
  def apply(): FileProcessor = {
    new FileProcessor()
  }
}
