package repositories

import cats.effect.{IO, Resource}
import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.util.fragment.Fragment.{const => csql}
import org.slf4j.LoggerFactory
import repositories.Repository.Credentials

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source


class IngestRepository(transactor: => Transactor[IO], private val credentials: Credentials) {
  private val logger = LoggerFactory.getLogger(classOf[IngestRepository])

  private val rootDir: String = {
    if (new File(credentials.csvBaseDir).isAbsolute) credentials.csvBaseDir
    else os.pwd.toString + "/" + credentials.csvBaseDir
  }

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
    logger.info(s"Ingesting files in $processDir for schema $schemaName")
    Future.sequence(
      recordToFileNames.map {
        case (t, f) => ingestFile(s"$schemaName.$t", s"$processDir/$f")
      }
    ).map(_.toList.size)
  }

  def ingestFile(table: String, filePath: String): Future[Long] = {
    logger.info(s"Ingest file $filePath into table $table")
    val in = Files.newInputStream(new File(filePath).toPath, StandardOpenOption.READ)
    PHC.pgGetCopyAPI(
      PFCM.copyIn(s"""COPY $table FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',');""", in)
    ).transact(transactor).unsafeToFuture()
  }

  def initialiseSchema(epoch: String): Future[String] = {
    logger.info(s"initialiseSchema($epoch)")
    (for {
      _             <- ensureStatusTableExists()
      schemasToDrop <- schemasToDrop()
      _             <- dropSchemas(schemasToDrop)
      schemaName    <- createSchema(epoch)
      _             <- createTables(schemaName)
      _             <- insertNewSchemaStatus(schemaName)
    } yield schemaName).unsafeToFuture()
  }

  private def ensureStatusTableExists(): IO[Int] =
    for {
      rs      <- resourceAsString("/create_db_address_status_table.sql")
      result  <- csql(rs).update.run.transact(transactor)
    } yield result

  private def schemasToDrop(): IO[List[String]] =
     for {
        rs      <- resourceAsString("/create_db_schemas_to_drop.sql")
        result  <- csql(rs).query[String].to[List].transact(transactor)
      } yield result

  private def dropSchemas(schemas: List[String]): IO[Int] = {
    val sqlStringRes = resourceAsString("/create_db_drop_schema.sql")

    schemas.map { schema =>
      for {
        sqlStr  <- sqlStringRes
        sql     <- csql(sqlStr.replaceAll("__schema__", schema)).update.run.transact(transactor)
      } yield sql
    }.fold(IO(0)) {
      case (ioa, iob) => for {
        a <- ioa
        b <- iob
      } yield a + b
    }
  }

  private val timestampFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")

  private def schemaNameFor(epoch: String) = {
    val timestamp = LocalDateTime.now(ZoneId.of("UTC"))
    s"ab${epoch}_${timestampFormat.format(timestamp)}"
  }

  private def createSchema(epoch: String) = {
    val schemaName = schemaNameFor(epoch)
    csql(s"CREATE SCHEMA IF NOT EXISTS $schemaName")
        .update
        .run
        .transact(transactor)
        .map(_ => schemaName)
  }

  def listSchemas: Future[List[String]] = {
    logger.info(s"listSchemas")
    sql"SELECT schema_name FROM information_schema.schemata"
        .query[String]
        .to[List]
        .transact(transactor)
        .unsafeToFuture()
  }

  private def createTables(schemaName: String): IO[Int] = {
    for {
      sqlt    <- resourceAsString("/create_db_schema.sql")
      sql     = sqlt.replaceAll("__schema__", schemaName)
      result  <- csql(sql).update.run.transact(transactor)
    } yield result
  }

  private def insertNewSchemaStatus(schemaName: String): IO[Int] = {
    sql"""INSERT INTO public.address_lookup_status(schema_name, status, timestamp)
         | VALUES($schemaName, 'schema_created', NOW())""".stripMargin
                                                          .update
                                                          .run
                                                          .transact(transactor)
  }

  def createLookupView(schemaName: String): Future[(Int, Int)] = {
    logger.info(s"Creating lookup view in schema $schemaName")
    (for {
      cfsqlt    <- resourceAsString("/create_db_lookup_view_and_indexes_function.sql")
      cfsql     = cfsqlt.replaceAll("__schema__", schemaName)
      icfsqlt   <- resourceAsString("/create_db_invoke_lookup_view_function.sql")
      icfsql    = icfsqlt.replaceAll("__schema__", schemaName)
      f         <- csql(cfsql).update.run.transact(transactor)
      v         <- csql(icfsql).update.run.transact(transactor)
    } yield (f, v)).unsafeToFuture()
  }

  def checkIfLookupViewCreated(schemaName: String): Future[Boolean] = {
    logger.info(s"Checking if lookup view has been created in schema $schemaName")

    (for {
      sqlt    <- resourceAsString("/check_lookup_view_created.sql")
      sql     = sqlt.mkString.replaceAll("__schema__", schemaName)
      created <- csql(sql).query[Boolean].unique.transact(transactor)
    } yield created).unsafeToFuture()
  }

  def checkLookupViewStatus(schemaName: String): Future[(String, Option[String])] = {
    logger.info(s"Checking status of schema $schemaName")
    sql"""SELECT status, error_message
         | FROM   public.address_lookup_status
         | WHERE  schema_name = $schemaName""".stripMargin
                                              .query[(String, Option[String])]
                                              .unique
                                              .transact(transactor)
                                              .unsafeToFuture()
  }

  def finaliseSchema(epoch: String, schemaName: String): Future[Boolean] = {
    logger.info(s"Finalising schema $schemaName for epoch $epoch")
    for {
      status  <- getSchemaStatus(schemaName)
      ok      <- isNewSchemaWithinChangeTolerance(schemaName)
      proceed = status._1 == "completed" && ok
      _       <- switchAddressLookupViewToNew(proceed, schemaName)
      _       = cleanupOldEpochDirectories(proceed, epoch)
      //      _ =  cleanupProcessedCsvs(proceed, epoch)
    } yield ok
  }

  private def switchAddressLookupViewToNew(proceed: Boolean, schemaName: String): Future[Int] = {
    if (!proceed) Future.successful(0)

    resourceAsString("/create_db_switch_public_view.sql")
      .map(s => csql(s.replaceAll("__schema__", schemaName)))
      .flatMap(_.update.run.transact(transactor))
      .unsafeToFuture()
  }

  private def cleanupOldEpochDirectories(proceed: Boolean, epoch: String): Unit = {
    if (proceed) {
      os.walk(
        path = os.Path(rootDir),
        skip = p => p.baseName == epoch,
        maxDepth = 1
      ).filter(_.toIO.isDirectory).foreach(os.remove.all)
    }
  }

  private def cleanupProcessedCsvs(proceed: Boolean, epoch: String): Unit = {
    if (proceed) {
      val epochDir = os.Path(rootDir) / epoch
      os.walk(
        path = epochDir,
        skip = p => {
          val pn = p.toIO.getName
          pn.endsWith(".csv") || pn == "processed.done"
        },
        maxDepth = 1
      )
    }
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
          latestCount       <- getCount(latestSchemaName)
          previousCount     <- getCount(previousSchemaName)
          percentageChange  = if (previousCount == 0) 0 else ((latestCount - previousCount) / previousCount) * 100.0
          withinTolerance   = 0.3 >= percentageChange && percentageChange >= 0
        } yield withinTolerance

      case None => Future.successful(true)
    }
  }

  private def getCount(schemaName: String): Future[Int] = {
    csql(s"SELECT COUNT(*) FROM ${schemaName}.abp_street_descriptor;")
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

  private def resourceAsString(name: String): IO[String] = {
    Resource.make[IO, Source](
      IO(Source.fromURL(getClass.getResource(name), "utf-8")))(s => IO(s.close()))
            .use(s => IO(s.mkString))
  }
}