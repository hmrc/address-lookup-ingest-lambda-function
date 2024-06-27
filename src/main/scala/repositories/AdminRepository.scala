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

import cats.effect.IO
import doobie._
import doobie.implicits._
import org.slf4j.LoggerFactory
import repositories.Repository.Credentials

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

class AdminRepository(transactor: => Transactor[IO],
                      private val credentials: Credentials) {
  private val logger = LoggerFactory.getLogger(classOf[AdminRepository])

  private val rootDir = credentials.csvBaseDir // not_used_currently

  def initialiseUsers(): Future[Unit] = {
    logger.info(s"initialiseUsers()")
    for {
      _ <- Future.successful("Do we need this?")
    } yield ()
  }

  def listUsers: Future[List[String]] = {
    sql"SELECT usename AS role_name FROM pg_catalog.pg_user"
      .query[String]
      .to[List]
      .transact(transactor)
      .unsafeToFuture()
  }

}
