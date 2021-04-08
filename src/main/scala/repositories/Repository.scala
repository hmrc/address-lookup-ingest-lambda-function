package repositories

/*
 * Copyright 2020 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cats.effect.{ContextShift, IO}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.rds.auth.{GetIamAuthTokenRequest, RdsIamAuthTokenGenerator}
import com.jessecoyle.JCredStash
import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

trait Repository {}

class AdminRepository(transactor: Transactor[IO]) {

  import Repository._

  def initialiseUsers(): Future[Unit] = {
    println(s"initialiseUsers()")
    for {
      _ <- initialiseIngestUser()
      _ <- initialiseReaderUser()
    } yield ()
  }

  def initialiseSchema(epoch: String): Future[String] = {
    println(s"initialiseSchema($epoch)")
    for {
      _ <- ensureStatusTableExists()
      schemasToDrop <- getSchemasToDrop()
      _ <- dropSchemas(schemasToDrop)
      schemaName <- createSchema(epoch)
      _ <- createTables(schemaName)
      _ <- insertNewSchemaStatus(schemaName)
    } yield schemaName
  }

  private def ensureStatusTableExists() = {
    sql"""CREATE TABLE IF NOT EXISTS public.address_lookup_status (
         |    schema_name VARCHAR(64) NOT NULL PRIMARY KEY,
         |    status      VARCHAR(32) NOT NULL,
         |    error_message VARCHAR NULL,
         |    timestamp   TIMESTAMP NOT NULL);""".stripMargin
                                                 .update.run
                                                 .transact(transactor)
                                                 .unsafeToFuture()
  }

  private def getSchemasToDrop() = {
    sql"""SELECT schema_name
         |FROM public.address_lookup_status
         |WHERE schema_name NOT IN (
         |    SELECT schema_name
         |    FROM public.address_lookup_status
         |    WHERE status = 'finalised'
         |    ORDER BY timestamp DESC
         |    LIMIT 1);""".stripMargin
                          .query[String]
                          .to[List]
                          .transact(transactor)
                          .unsafeToFuture()
  }

  private def dropSchemas(schemas: List[String]) = {
    Future.sequence(
      schemas
        .map(schema =>
          Fragment.const(
            s"""DROP SCHEMA IF EXISTS $schema CASCADE;
               | DELETE FROM public.address_lookup_status
               | WHERE schema_name = '$schema';""".stripMargin)
        )
        .map { ssql =>
          ssql.update.run.transact(transactor).unsafeToFuture()
        }
    )
  }

  private val timestampFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")

  private def schemaNameFor(epoch: String) = {
    val timestamp = LocalDateTime.now(ZoneId.of("UTC"))
    s"ab${epoch}_${timestampFormat.format(timestamp)}"
  }

  private def createSchema(epoch: String) = {
    val schemaName = schemaNameFor(epoch)
    Fragment.const(s"CREATE SCHEMA IF NOT EXISTS $schemaName")
            .update
            .run
            .transact(transactor)
            .unsafeToFuture()
            .map(_ => schemaName)
  }

  def listSchemas: Future[List[String]] = {
    println(s"listSchemas")
    sql"SELECT schema_name FROM information_schema.schemata"
      .query[String]
      .to[List]
      .transact(transactor)
      .unsafeToFuture()
  }

  private def createTables(schemaName: String): Future[Int] = {
    val createSchemaSql =
      Source.fromResource("create_db_schema.sql").mkString.replaceAll("__schema__", schemaName)
    Fragment.const(createSchemaSql).update.run.transact(transactor).unsafeToFuture()
  }

  private def insertNewSchemaStatus(schemaName: String): Future[Int] = {
    sql"""INSERT INTO public.address_lookup_status(schema_name, status, timestamp)
         | VALUES($schemaName, 'schema_created', NOW())""".stripMargin
                                                          .update
                                                          .run
                                                          .transact(transactor)
                                                          .unsafeToFuture()
  }

  private def initialiseIngestUser() = {
    val ingestorUser = Credentials().ingestor
    val database = Credentials().database
    sql"SELECT usename FROM pg_user WHERE usename = $ingestorUser"
      .query[String]
      .option
      .transact(transactor)
      .unsafeToFuture()
      .flatMap {
        case None    =>
          sql"""REVOKE CREATE ON SCHEMA public FROM PUBLIC;
               | CREATE USER $ingestorUser;
               | GRANT rds_iam TO $ingestorUser;
               | GRANT ALL ON DATABASE $database TO $ingestorUser;
               | GRANT CREATE ON SCHEMA public TO $ingestorUser;
               |""".stripMargin.update.run
                   .transact(transactor)
                   .unsafeToFuture()
        case Some(_) =>
          Future.successful(println(s"'ingestor' user already exists"))
      }
  }

  private def initialiseReaderUser() = {
    val readerUser = Credentials().reader
    val readerPassword = Credentials().readerPassword
    val database = Credentials().database
    sql"SELECT usename FROM pg_user WHERE usename = $readerUser"
      .query[String]
      .option
      .transact(transactor)
      .unsafeToFuture()
      .flatMap {
        case None    =>
          sql"""CREATE USER $readerUser ENCRYPTED PASSWORD '$readerPassword';
               |GRANT CONNECT ON DATABASE $database TO $readerUser;""".stripMargin
                                                                      .update.run
                                                                      .transact(transactor)
                                                                      .unsafeToFuture()
        case Some(_) =>
          Future.successful(println(s"'reader' user already exists"))
      }
  }

  def listUsers: Future[List[String]] = {
    println(s"listUsers")
    sql"SELECT usename AS role_name FROM pg_catalog.pg_user"
      .query[String]
      .to[List]
      .transact(transactor)
      .unsafeToFuture()
  }

}

class IngestRepository(transactor: Transactor[IO]) {

  import Repository._

  val rootDir = "/mnt/efs/"

  // Does this belong here???
  val recordToFileNames = Map(
    "abp_blpu" -> "ID21_BLPU_Records.csv",
    "abp_delivery_point" -> "ID28_DPA_Records.csv",
    "abp_lpi" -> "ID24_LPI_Records.csv",
    "abp_crossref" -> "ID23_XREF_Records.csv",
    "abp_classification" -> "ID32_Class_Records.csv",
    "abp_street" -> "ID11_Street_Records.csv",
    "abp_street_descriptor" -> "ID15_StreetDesc_Records.csv",
    "abp_organisation" -> "ID31_Org_Records.csv",
    "abp_successor" -> "ID30_Successor_Records.csv"
  )

  def ingestFiles(schemaName: String, processDir: String): Future[Int] = {
    println(s"ingestFiles($schemaName, $processDir)")
    Future.sequence(
      recordToFileNames.map {
        case (t, f) => ingestFile(s"$schemaName.$t", s"$processDir/$f")
      }
    ).map(_.toList.size)
  }

  def ingestFile(table: String, filePath: String): Future[Long] = {
    println(s"ingestFile($table, $filePath)")
    val in = Files.newInputStream(new File(filePath).toPath, StandardOpenOption.READ)
    PHC.pgGetCopyAPI(
      PFCM.copyIn(s"""COPY $table FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',');""", in)
    ).transact(transactor).unsafeToFuture()
  }

  def createLookupView(schemaName: String): Future[(Int, Int)] = {
    println(s"createLookupView($schemaName)")
    val createViewSql =
      Source.fromURL(getClass.getResource("/create_db_lookup_view_and_indexes.sql"))
            .mkString.replaceAll("__schema_name__", schemaName)

    (for {
      f <- Fragment.const(createViewSql).update.run.transact(transactor)
      v <- Fragment.const(
        s"""BEGIN TRANSACTION;
           | CALL $schemaName.create_address_lookup_view('$schemaName');
           | COMMIT;""".stripMargin).update.run.transact(transactor)
    } yield (f, v)).unsafeToFuture()
  }

  def checkIfLookupViewCreated(schemaName: String): Future[Boolean] = {
    println(s"checkIfLookupViewCreated($schemaName)")
    sql"""SELECT EXISTS(
         |   SELECT 1
         |   FROM pg_matviews
         |     WHERE schemaname = $schemaName
         |   AND matviewname = 'address_lookup')""".stripMargin
                                                   .query[Boolean]
                                                   .unique
                                                   .transact(transactor)
                                                   .unsafeToFuture()
  }

  def checkLookupViewStatus(schemaName: String): Future[(String, Option[String])] = {
    println(s"checkLookupViewStatus($schemaName)")
    sql"""SELECT status, error_message
         | FROM   public.address_lookup_status
         | WHERE  schema_name = $schemaName""".stripMargin
                                              .query[(String, Option[String])]
                                              .unique
                                              .transact(transactor)
                                              .unsafeToFuture()
  }

  def finaliseSchema(epoch: String, schemaName: String): Future[Boolean] = {
    println(s"finaliseSchema($epoch, $schemaName)")
    for {
      status <- getSchemaStatus(schemaName)
      ok <- isNewSchemaWithinChangeTolerance(schemaName)
      proceed = status._1 == "completed" && ok
      _ <- switchAddressLookupViewToNew(proceed, schemaName)
      _ =  cleanupOldEpochDirectories(proceed, epoch)
    } yield ok
  }

  private def switchAddressLookupViewToNew(proceed: Boolean, schemaName: String): Future[Int] = {
    if (!proceed) Future.successful(0)

    Fragment.const(
      s"""CREATE OR REPLACE VIEW public.address_lookup AS SELECT * FROM ${schemaName}.address_lookup;
         | GRANT SELECT ON public.address_lookup TO addresslookupreader;
         | UPDATE public.address_lookup_status SET status = 'finalised' WHERE schema_name = '$schemaName';"""
        .stripMargin)
            .update
            .run
            .transact(transactor)
            .unsafeToFuture()
  }

  private def cleanupOldEpochDirectories(proceed: Boolean, epoch: String): Unit = {
    os.walk(
      path = os.Path(rootDir),
      skip = _.baseName == epoch,
      maxDepth = 1
    ).foreach(os.remove.all)
  }

  private def getSchemaStatus(schemaName: String): Future[(String, Option[String])] = {
    sql"""SELECT status, error_message
         | FROM  public.address_lookup_status
         | WHERE schema_name = $schemaName""".stripMargin
                                             .query[(String, Option[String])]
                                             .unique
                                             .transact(transactor)
                                             .unsafeToFuture()
  }

  private def isNewSchemaWithinChangeTolerance(latestSchemaName: String): Future[Boolean] = {
    getSchemaToCompare(latestSchemaName).flatMap {
      case Some(previousSchemaName) =>
        for {
          latestCount <- getCount(latestSchemaName)
          previousCount <- getCount(previousSchemaName)
          percentageChange = ((latestCount - previousCount) / previousCount) * 100.0
          withinTolerance = 0.3 >= percentageChange && percentageChange >= 0
        } yield withinTolerance

      case None => Future.successful(true)
    }
  }

  private def getCount(schemaName: String): Future[Int] = {
    Fragment.const(s"SELECT COUNT(*) FROM ${schemaName}.abp_street_descriptor;")
            .query[Int].unique.transact(transactor).unsafeToFuture()
  }

  private def getSchemaToCompare(latestSchemaName: String): Future[Option[String]] = {
    sql"""SELECT schema_name
         | FROM   public.address_lookup_status
         | WHERE status = 'finalised'
         | AND   schema_name <> $latestSchemaName
         | ORDER BY timestamp DESC
         | LIMIT 1;""".stripMargin
                      .query[String]
                      .option
                      .transact(transactor)
                      .unsafeToFuture()
  }
}

object Repository {
  def forAdmin(): AdminRepository = new AdminRepository(adminTransactor)

  def forIngest(): IngestRepository = new IngestRepository(ingestorTransactor)

  private lazy val adminTransactor: Transactor[IO] = adminXa()
  private lazy val ingestorTransactor: Transactor[IO] = ingestorXa()

  private def adminXa(): Transactor[IO] = {
    implicit val cs: ContextShift[IO] =
      IO.contextShift(implicitly[ExecutionContext])

    val dbCfg = adminConnectionConfig

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${dbCfg.host}:5433/addressbasepremium",
      dbCfg.username,
      dbCfg.password
    )
  }

  private def ingestorXa(): Transactor[IO] = {
    implicit val cs: ContextShift[IO] =
      IO.contextShift(implicitly[ExecutionContext])

    val dbCfg = ingestorConnectionConfig

    Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      s"jdbc:postgresql://${dbCfg.host}:5433/${dbCfg.database}",
      dbCfg.username,
      dbCfg.password
    )
  }

  case class DbCfg(host: String, database: String, username: String, password: String)

  private def adminConnectionConfig = {
    val creds = Credentials()
    DbCfg(
      host = creds.host,
      database = creds.database,
      username = creds.admin,
      password = creds.adminPassword
    )
  }

  private def ingestorConnectionConfig = {
    val creds = Credentials()
    DbCfg(
      host = creds.host,
      database = creds.database,
      username = creds.ingestor,
      password = creds.ingestorToken
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

    def database = "addressbasepremium"

    def port = 5433

    def admin = "root"

    def adminPassword = "password"

    def ingestor = admin

    def ingestorToken = adminPassword

    def reader = admin

    def readerPassword = adminPassword
  }

  class RdsCredentials() extends Credentials {
    def credStash = new JCredStash()

    val context = Map("role" -> "address_lookup_file_download").asJava

    def host = credStash.getSecret("address_lookup_rds_host", context)

    def database =
      credStash.getSecret("address_lookup_rds_database", context)

    def admin =
      credStash.getSecret("address_lookup_rds_admin_user", context)

    def adminPassword =
      credStash.getSecret("address_lookup_rds_admin_password", context)

    def ingestor =
      credStash.getSecret("address_lookup_rds_ingest_user", context)

    def ingestorToken =
      generateAuthToken("eu-west-2", host, "5432", ingestor)

    def reader =
      credStash.getSecret("address_lookup_rds_readonly_user", context)

    def readerPassword =
      credStash.getSecret("address_lookup_rds_readonly_password", context)

  }

}
