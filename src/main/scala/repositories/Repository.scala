package repositories

import cats.effect.{ContextShift, IO}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.rds.auth.{GetIamAuthTokenRequest, RdsIamAuthTokenGenerator}
import com.jessecoyle.JCredStash
import doobie.Transactor

import java.util
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConverters._

object Repository {
  def forAdmin(): AdminRepository = new AdminRepository(adminTransactor)
  def forIngest(): IngestRepository = new IngestRepository(ingestorTransactor)
  private def creds: Credentials = Credentials()

  def adminTransactor: Transactor[IO] = adminXa(creds)
  def ingestorTransactor: Transactor[IO] = ingestorXa(creds)

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
      creds.ingestor,
      creds.ingestorToken
    )
  }

  private def generateAuthToken(
                                 region: String,
                                 hostName: String,
                                 port: String,
                                 username: String
                               ) = {
    val generator = RdsIamAuthTokenGenerator.builder
                                            .credentials(new DefaultAWSCredentialsProviderChain)
                                            .region(region)
                                            .build

    val authToken = generator.getAuthToken(
      GetIamAuthTokenRequest.builder
                            .hostname(hostName)
                            .port(port.toInt)
                            .userName(username)
                            .build
    )

    authToken
  }

  trait Credentials {
    def host: String

    def port: String

    def database: String

    def admin: String

    def adminPassword: String

    def ingestor: String

    def ingestorToken: String

    def reader: String

    def readerPassword: String
  }

  object Credentials {
    def apply(): Credentials = {
      new RdsCredentials()
    }
  }

  class LocalCredentials() extends Credentials {
    def host = "localhost"

    def port = "5433"

    def database: String = "addressbasepremium"

    def admin = "root"

    def adminPassword = "password"

    def ingestor = admin

    def ingestorToken = adminPassword

    def reader = admin

    def readerPassword = adminPassword
  }

  class RdsCredentials() extends Credentials {

    val context: util.Map[String, String] =
      Map("role" -> "address_lookup_file_download").asJava

    private def retrieveCredentials(credential: String) = {
      println(s">>> retrieveCredentials($credential)")
      val credStash = new JCredStash()
      println(s">>> JCredstash: ${credStash.listSecrets()}")
      credStash.getSecret(credential, context)
    }

    def host: String = {
      retrieveCredentials("address_lookup_rds_host")
    }

    def port: String = "5432"

    def database: String =
      retrieveCredentials("address_lookup_rds_database")

    def admin: String =
      retrieveCredentials("address_lookup_rds_admin_user")

    def adminPassword: String =
      retrieveCredentials("address_lookup_rds_admin_password")

    def ingestor: String =
      retrieveCredentials("address_lookup_rds_ingest_user")

    def ingestorToken: String =
      generateAuthToken("eu-west-2", host, "5432", ingestor)

    def reader: String =
      retrieveCredentials("address_lookup_rds_readonly_user")

    def readerPassword: String =
      retrieveCredentials("address_lookup_rds_readonly_password")

  }

}
