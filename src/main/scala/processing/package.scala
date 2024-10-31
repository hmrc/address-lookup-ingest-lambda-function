import java.io.File
import java.nio.file.{Files, Path}
import java.security.{DigestInputStream, MessageDigest}

package object processing {
  object implicits {
    implicit class RichFile(f: File) {
      def ensureDirsExist(): File = {
        f.mkdirs()
        f
      }

      def checkMd5(requiredMd5: String): Boolean = {
        MessageDigest.getInstance("MD5").md5Matches(f, requiredMd5)
      }

      def checkMinSize: Boolean = {
        val actualSize = f.length()
        val minSize = minSizeFor(f.getName)
        println(s">>> actualSize: ${actualSize}, minSize: ${minSize}")
        actualSize > minSize
      }

      private val filePattern = "AB..(GB|IS)_CSV.zip".r
      private def minSizeFor(fileName: String): Long = fileName match {
        case filePattern("GB") => 9500000000L
        case filePattern("IS") => 150000000L
      }
    }

    implicit class RichPath(p: Path) {
      def ensureDirsExist(): Path = {
        val pp = if (p.toFile.isFile) p.getParent else p
        pp.toFile.mkdirs()
        p
      }
    }

    implicit class RichMessageDigest(md: MessageDigest) {
      def md5ForFile(f: File): String = {
        val buffer = new Array[Byte](4096)
        val dis = new DigestInputStream(Files.newInputStream(f.toPath), md)
        while (dis.available > 0) {
          dis.read(buffer)
        }
        md.digest.map(b => String.format("%02x", Byte.box(b))).mkString
      }

      def md5Matches(f: File, md5: String): Boolean = {
        val actualMd5 = md5ForFile(f)
        println(s">>> actualMd5: ${actualMd5}, requiredMd5: ${md5}")
        actualMd5 == md5
      }
    }

    implicit class RichString(s: String) {
      def dbSafe: String =
        s.replaceAll("AddressBase Premium ", "ABP")
          .replaceAll("\\.", "")

    }
  }
}
