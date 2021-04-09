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
import repositories.Repository.Credentials

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

class AdminRepository(transactor: => Transactor[IO], private val credentials: Credentials) {

  import Repository._

  private val rootDir = credentials.csvBaseDir // not_used_currently

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
    val ingestorUser = credentials.ingestor
    val database = credentials.database
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
    val readerUser = credentials.reader
    val readerPassword = credentials.readerPassword
    val database = credentials.database
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


