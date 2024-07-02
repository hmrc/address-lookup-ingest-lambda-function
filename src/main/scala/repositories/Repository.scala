package repositories

import cats.effect.{ContextShift, IO}
import doobie.Transactor
import me.lamouri.JCredStash

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

object Repository {
  case class Repositories(forAdmin: AdminRepository,
                          forIngest: IngestRepository)

  def apply(): Repositories = repositories(Credentials())
  def forTesting(): Repositories =
    repositories(Credentials.forTesting())

  private def repositories(credentials: Credentials): Repositories = {
    val adminTransactor: Transactor[IO] = adminXa(credentials)
    val ingestorTransactor: Transactor[IO] = ingestorXa(credentials)

    Repositories(
      forAdmin = new AdminRepository(adminTransactor, credentials),
      forIngest = new IngestRepository(ingestorTransactor, credentials)
    )
  }

  private def adminXa(creds: Credentials): Transactor[IO] = {
    implicit val cs: ContextShift[IO] =
      IO.contextShift(implicitly[ExecutionContext])

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      creds.admin,
      creds.adminPassword
    )
  }

  private def ingestorXa(creds: Credentials): Transactor[IO] = {
    implicit val cs: ContextShift[IO] =
      IO.contextShift(implicitly[ExecutionContext])

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      creds.admin,
      creds.adminPassword
    )
  }

  sealed trait Credentials {
    def host: String
    def port: String
    def database: String
    def admin: String
    def adminPassword: String
    def csvBaseDir: String
  }

  object Credentials {
    def apply(): Credentials = {
      new RdsCredentials()
    }

    def forTesting(): Credentials = {
      new LocalCredentials()
    }
  }

  final class LocalCredentials() extends Credentials {
    override def host: String = "localhost"
    override def port: String = "5433"
    override def database: String = "addressbasepremium"
    override def admin: String = "root"
    override def adminPassword: String = "password"
    override def csvBaseDir: String = "src/test/resources/csv"
  }

  final class RdsCredentials() extends Credentials {
    private val credStashPrefix = sys.env.getOrElse("CREDSTASH_PREFIX", "")

    private val credstashTableName = "credential-store"
    private val context: util.Map[String, String] =
      Map("role" -> "cip_address_search").asJava

    private val lookupContext: util.Map[String, String] =
      Map("role" -> "address_lookup_file_download").asJava

    private def retrieveCredentials(
      credential: String,
      context: util.Map[String, String] = context
    ) = {
      val credStash = new JCredStash()
      credStash.getSecret(credstashTableName, credential, context).trim
    }

    override def host: String =
      retrieveCredentials(s"address_search_rds_rw_host")
    override def port: String = "5432"
    override def database: String =
      retrieveCredentials(s"address_search_rds_database")
    override def admin: String =
      retrieveCredentials(s"address_search_rds_admin_user")
    override def adminPassword: String =
      retrieveCredentials(s"address_search_rds_admin_password")
    override def csvBaseDir: String = "/mnt/efs/"
  }
}
