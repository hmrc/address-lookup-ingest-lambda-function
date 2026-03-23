package lambdas

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar
import repositories.IngestRepository

import scala.concurrent.Future

class AddressLookupFinaliseSchemaLambdaFunctionTest extends AsyncWordSpec with Matchers with MockitoSugar {
  "AddressLookupFinaliseSchemaLambdaFunction" should {
    "return true" when {
      "the schema status is completed and the new schema is within change tolerance" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.finaliseSchema(meq("10"), meq("ab10_20210608_101010")))
          .thenReturn(Future.successful(true))

        new AddressLookupFinaliseSchemaLambdaFunction()
          .finaliseSchema(mockRepo, "10", "ab10_20210608_101010")
          .map(result => result shouldBe true)
      }
    }

    "return false" when {
      "the repository indicates the schema is not within change tolerance" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.finaliseSchema(meq("10"), meq("ab10_20210608_101010")))
          .thenReturn(Future.successful(false))

        new AddressLookupFinaliseSchemaLambdaFunction()
          .finaliseSchema(mockRepo, "10", "ab10_20210608_101010")
          .map(result => result shouldBe false)
      }
    }

    "propagate a failure" when {
      "the repository throws an exception" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.finaliseSchema(meq("10"), meq("ab10_20210608_101010")))
          .thenReturn(Future.failed(new RuntimeException("db connection lost")))

        recoverToSucceededIf[RuntimeException] {
          new AddressLookupFinaliseSchemaLambdaFunction()
            .finaliseSchema(mockRepo, "10", "ab10_20210608_101010")
        }
      }
    }

    "pass the epoch and schema name unchanged to the repository" when {
      "finaliseSchema is called" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.finaliseSchema(meq("99"), meq("ab99_20260101_120000")))
          .thenReturn(Future.successful(true))

        new AddressLookupFinaliseSchemaLambdaFunction()
          .finaliseSchema(mockRepo, "99", "ab99_20260101_120000")
          .map(result => result shouldBe true)
      }
    }
  }
}

