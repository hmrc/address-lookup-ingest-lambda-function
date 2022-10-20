package repositories

import cats.effect.IO
import doobie._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SchemaToleranceChecker(count: String => Future[Int]) {
  def withinTolerance(newSchemaName: String, existingSchemaName: String, tolerancePercent: Double)(implicit transactor: Transactor[IO]): Future[Boolean] =
    for {
      newSchemaSize <- count(newSchemaName)
      existingSchemaSize <- count(existingSchemaName)
    } yield {
      val percentageChange = Math.abs(
        if (existingSchemaSize == 0) 0.0
        else ((newSchemaSize.toDouble - existingSchemaSize.toDouble) / existingSchemaSize.toDouble) * 100.0
      )
      tolerancePercent >= percentageChange && percentageChange >= 0.0
    }
}
