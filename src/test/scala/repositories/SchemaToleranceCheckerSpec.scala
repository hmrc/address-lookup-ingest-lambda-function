package repositories

import cats.effect.IO
import doobie.util.transactor.Transactor
import org.mockito.ArgumentMatchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SchemaToleranceCheckerSpec extends AnyFlatSpec with Matchers with MockitoSugar with OptionValues {
  behavior of "SchemaToleranceChecker"

  implicit val mockTransactor: Transactor[IO] = mock[Transactor[IO]]
  val tolerancePercent = 0.3

  var countValues: Map[String,Int] = Map();
  def getCount(str: String): Future[Int] = Future.successful(countValues(str))

  it should "validate when new schema is smaller than old schema and within tolerance" in {
    val checker = new SchemaToleranceChecker(getCount)
    countValues = countValues.empty
    countValues += ("new-schema" -> 1000000)
    countValues += ("old-schema" -> 1000200)

    val result = Await.result(
      checker.withinTolerance("new-schema", "old-schema", tolerancePercent), 5.seconds)
    result shouldBe true
  }

  it should "validate when new schema is larger than old schema and within tolerance" in {
    val checker = new SchemaToleranceChecker(getCount)
    countValues = countValues.empty
    countValues += ("old-schema" -> 1000000)
    countValues += ("new-schema" -> 1000200)

    val result = Await.result(
      checker.withinTolerance("new-schema", "old-schema", tolerancePercent), 5.seconds)
    result shouldBe true
  }

  it should "validate when new schema is the same size as the old schema" in {
    val checker = new SchemaToleranceChecker(getCount)
    countValues = countValues.empty
    countValues += ("old-schema" -> 1000000)
    countValues += ("new-schema" -> 1000000)

    val result = Await.result(
      checker.withinTolerance("new-schema", "old-schema", tolerancePercent), 5.seconds)
    result shouldBe true
  }

  it should "not validate when new schema is smaller than old schema but outside tolerance" in {
    val checker = new SchemaToleranceChecker(getCount)
    countValues = countValues.empty
    countValues += ("new-schema" -> 1000000)
    countValues += ("old-schema" -> 1100000)

    val result = Await.result(
      checker.withinTolerance("new-schema", "old-schema", tolerancePercent*10.0F), 5.seconds)
    result shouldBe false
  }

  it should "not validate when new schema is larger than old schema but outside tolerance" in {
    val checker = new SchemaToleranceChecker(getCount)
    countValues = countValues.empty
    countValues += ("old-schema" -> 1000000)
    countValues += ("new-schema" -> 1100000)

    val result = Await.result(
      checker.withinTolerance("new-schema", "old-schema", tolerancePercent*10.0F), 5.seconds)
    result shouldBe false
  }
}
