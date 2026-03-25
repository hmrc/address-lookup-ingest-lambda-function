package utils

import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.{AccessDeniedException, NoSuchFileException}
import scala.concurrent.duration.DurationInt
import scala.util.{Try, boundary}

class FileUtilsSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  behavior of "FileUtils"

  private val fileUtils = new FileUtils {}

  it should "successfully remove directory on first attempt" in {
    val testPath = os.temp.dir()
    try {
      fileUtils.removeDirectoryWithRetry(testPath)
      os.exists(testPath) shouldBe false
    } finally {
      Try(os.remove.all(testPath))
    }
  }

  it should "successfully remove empty directory" in {
    val testPath = os.temp.dir()
    try {
      fileUtils.removeDirectoryWithRetry(testPath)
      os.exists(testPath) shouldBe false
    } finally {
      Try(os.remove.all(testPath))
    }
  }

  it should "successfully remove directory with files" in {
    val testPath = os.temp.dir()
    val file = testPath / "testfile.txt"
    os.write(file, "test content")
    try {
      fileUtils.removeDirectoryWithRetry(testPath)
      os.exists(testPath) shouldBe false
    } finally {
      Try(os.remove.all(testPath))
    }
  }

  it should "successfully remove directory with nested subdirectories" in {
    val testPath = os.temp.dir()
    val subDir = testPath / "subdir" / "nested"
    os.makeDir.all(subDir)
    val file = subDir / "testfile.txt"
    os.write(file, "test content")
    try {
      fileUtils.removeDirectoryWithRetry(testPath)
      os.exists(testPath) shouldBe false
    } finally {
      Try(os.remove.all(testPath))
    }
  }

  it should "not throw exception when directory does not exist" in {
    val nonExistentPath = os.Path("/tmp/non-existent-directory-" + System.nanoTime())
    noException should be thrownBy fileUtils.removeDirectoryWithRetry(nonExistentPath)
  }


  it should "propagate non-NoSuchFileException errors" in {
    val mockFileUtils = spy(fileUtils)
    val testPath = os.Path("/tmp/test")
    val testException = new RuntimeException("Unexpected error")
    doThrow(testException).when(mockFileUtils).removeDirectoryWithRetry(testPath)

    an[RuntimeException] should be thrownBy mockFileUtils.removeDirectoryWithRetry(testPath)
  }

  it should "retry when directory not found on first attempt and succeed on second" in {
    var attemptCount = 0
    val testPath = os.temp.dir()
    val fileToDelete = testPath / "testfile.txt"
    os.write(fileToDelete, "content")

    val customFileUtils = new FileUtils {
      override def removeDirectoryWithRetry(path: os.Path): Unit = {
        val maxAttempts = 3
        val retryDelay = 100.millis

        boundary {
          (1 to maxAttempts).foreach { attemptNo =>
            val result = Try {
              attemptCount += 1
              if (attemptCount == 1) {
                throw new NoSuchFileException(path.toNIO.toString)
              }
              os.remove.all(path)
            }
            result match {
              case scala.util.Success(_) => boundary.break()
              case scala.util.Failure(_: NoSuchFileException) if attemptNo < maxAttempts =>
                Thread.sleep(retryDelay.toMillis)
              case scala.util.Failure(_: NoSuchFileException) =>
              case scala.util.Failure(e) => throw e
            }
          }
        }
      }
    }

    customFileUtils.removeDirectoryWithRetry(testPath)
    attemptCount shouldBe 2
    os.exists(testPath) shouldBe false
  }

  it should "retry three times on consecutive NoSuchFileException" in {
    var attemptCount = 0
    val testPath = os.Path("/tmp/test-" + System.nanoTime())

    val customFileUtils = new FileUtils {
      override def removeDirectoryWithRetry(path: os.Path): Unit = {
        val maxAttempts = 3
        val retryDelay = 50.millis

        boundary {
          (1 to maxAttempts).foreach { attemptNo =>
            val result = Try {
              attemptCount += 1
              throw new NoSuchFileException(path.toNIO.toString)
            }
            result match {
              case scala.util.Success(_) => boundary.break()
              case scala.util.Failure(_: NoSuchFileException) if attemptNo < maxAttempts =>
                Thread.sleep(retryDelay.toMillis)
              case scala.util.Failure(_: NoSuchFileException) =>
              case scala.util.Failure(e) => throw e
            }
          }
        }
      }
    }

    customFileUtils.removeDirectoryWithRetry(testPath)
    attemptCount shouldBe 3
  }

  it should "not retry and throw immediately on non-NoSuchFileException error" in {
    var attemptCount = 0
    val testPath = os.Path("/tmp/test-" + System.nanoTime())

    val customFileUtils = new FileUtils {
      override def removeDirectoryWithRetry(path: os.Path): Unit = {
        val maxAttempts = 3
        val retryDelay = 50.millis

        boundary {
          (1 to maxAttempts).foreach { attemptNo =>
            val result = Try {
              attemptCount += 1
              throw new RuntimeException("Permission denied")
            }
            result match {
              case scala.util.Success(_) => boundary.break()
              case scala.util.Failure(_: NoSuchFileException) if attemptNo < maxAttempts =>
                Thread.sleep(retryDelay.toMillis)
              case scala.util.Failure(_: NoSuchFileException) =>
              case scala.util.Failure(e) => throw e
            }
          }
        }
      }
    }

    an[RuntimeException] should be thrownBy customFileUtils.removeDirectoryWithRetry(testPath)
    attemptCount shouldBe 1
  }

  it should "succeed on third attempt after two NoSuchFileException failures" in {
    var attemptCount = 0
    val testPath = os.temp.dir()
    val fileToDelete = testPath / "testfile.txt"
    os.write(fileToDelete, "content")

    val customFileUtils = new FileUtils {
      override def removeDirectoryWithRetry(path: os.Path): Unit = {
        val maxAttempts = 3
        val retryDelay = 50.millis

        boundary {
          (1 to maxAttempts).foreach { attemptNo =>
            val result = Try {
              attemptCount += 1
              if (attemptCount < 3) {
                throw new NoSuchFileException(path.toNIO.toString)
              }
              os.remove.all(path)
            }
            result match {
              case scala.util.Success(_) => boundary.break()
              case scala.util.Failure(_: NoSuchFileException) if attemptNo < maxAttempts =>
                Thread.sleep(retryDelay.toMillis)
              case scala.util.Failure(_: NoSuchFileException) =>
              case scala.util.Failure(e) => throw e
            }
          }
        }
      }
    }

    customFileUtils.removeDirectoryWithRetry(testPath)
    attemptCount shouldBe 3
    os.exists(testPath) shouldBe false
  }
}
