package repositories

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
import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source


class IngestRepository(transactor: => Transactor[IO]) {

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
