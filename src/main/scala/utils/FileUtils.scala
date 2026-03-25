package utils

import org.slf4j.LoggerFactory

import java.nio.file.NoSuchFileException
import scala.concurrent.duration.DurationInt
import scala.util.{Try, boundary}

trait FileUtils {

  private val logger = LoggerFactory.getLogger(classOf[FileUtils])

  def removeDirectoryWithRetry(path: os.Path): Unit = {
    val maxAttempts = 3
    val retryDelay = 30.seconds

    boundary {
      (1 to maxAttempts).foreach { attemptNo =>
        val result = Try(os.remove.all(path))
        result match {
          case scala.util.Success(_) => boundary.break()
          case scala.util.Failure(_: NoSuchFileException) if attemptNo < maxAttempts =>
            logger.warn(
              s"Directory not found during cleanup (attempt $attemptNo/$maxAttempts): $path. Retrying in ${retryDelay.toSeconds} seconds."
            )
            Thread.sleep(retryDelay.toMillis)
          case scala.util.Failure(_: NoSuchFileException) =>
            logger.warn(
              s"Directory not found during cleanup after $maxAttempts attempts: $path"
            )
          case scala.util.Failure(e) => throw e
        }
      }
    }
  }
}
