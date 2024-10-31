package lambdas

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import processing.FileProcessor

import java.util.{List => jList, Map => jMap}
import scala.collection.JavaConverters._

class AddressLookupUnpackAndBatchFilesFunction extends RequestHandler[java.util.Map[String, Object], jMap[String, Object]] {
  override def handleRequest(batchInfo: java.util.Map[String, Object], context: Context): jMap[String, Object] = {
    val batchesRootDir = batchInfo.get("batchesRootDir").asInstanceOf[String]
    val unpackRequired = batchInfo.get("unpack").asInstanceOf[String].toBoolean
    val epochOverride = batchInfo.asScala.get("epoch").map(_.asInstanceOf[String])
    val downloadedFiles = batchInfo.get("downloadedFiles").asInstanceOf[jList[String]].asScala.toList

    doUnpackAndBatchFiles(batchesRootDir, unpackRequired, epochOverride, downloadedFiles)
  }

  def doUnpackAndBatchFiles(batchesRootDir: String, unpackRequired: Boolean, epochOverride: Option[String], downloadedFiles: List[String]): jMap[String, Object] = {
    if (unpackRequired) {
      val processor = FileProcessor()
      val result = processor.process(downloadedFiles, batchesRootDir)
      val jResult = result.map {
        case (bd, bs) => Map("batchDir" -> bd.getAbsolutePath, "batchFiles" -> bs.map(p =>
          p.toFile.getAbsolutePath).toList.asJava).asJava
      }.toList.asJava
      val epoch = result.head._1.getParentFile.getName

      Map("batches" -> jResult, "epoch" -> epoch).asJava
    } else {
      val batchDir = s"${batchesRootDir}/0"
      Map("batches" -> List(
        Map("batchDir" -> batchDir,
          "batchFiles" -> FileProcessor().filesInBatch(batchDir).asJava
        ).asJava,
      ).asJava,
        "epoch" -> epochOverride.get
      ).asJava
    }
  }
}
