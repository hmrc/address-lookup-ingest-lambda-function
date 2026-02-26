package repositories

import cats.effect.IO
import com.amazonaws.secretsmanager.caching.SecretCache
import doobie.Transactor
import me.lamouri.JCredStash
import services.SecretsManagerService
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient

import java.util
import scala.jdk.CollectionConverters.*
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

  private def adminXa(creds: Credentials): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      creds.admin,
      creds.adminPassword,
      None
    )

  private def ingestorXa(creds: Credentials): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${creds.host}:${creds.port}/${creds.database}",
      creds.admin,
      creds.adminPassword,
      None
    )

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
    private val awsClientBuilder: SecretsManagerClient = SecretsManagerClient
      .builder()
      .region(Region.EU_WEST_2)
      .build()
    private val secretsManagerService = new SecretsManagerService(new SecretCache(awsClientBuilder))

    private def retrieveCredentials(
      credential: String
    ) = {
      secretsManagerService.getSecret("rds/cip-address-search-api-rds-cluster/root", credential)
    }

    override def host: String = "address_search_rds_rw_host"
    override def port: String = "5432"
    override def database: String = "addressbasepremium"
    override def admin: String = retrieveCredentials("username")
    override def adminPassword: String = retrieveCredentials("password")
    override def csvBaseDir: String = "/mnt/efs/"
  }
}
