package lambdas

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar
import repositories.IngestRepository

import scala.concurrent.Future

class AddressLookupCheckStatusLambdaFunctionTest extends AsyncWordSpec with Matchers with MockitoSugar {

  "AddressLookupCheckStatusLambdaFunction" should {
    "return status information in the correct format" when {
      "status is completed and there is no error message" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.checkLookupViewStatus(meq("10"))).thenReturn(Future.successful(("completed", None)))

        new AddressLookupCheckStatusLambdaFunction().checkLookupViewStatus(mockRepo, "10")
            .map( result => result shouldBe Map("status" -> "completed", "errorMessage" -> null))
      }

      "status is errored and there is an error message" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.checkLookupViewStatus(meq("10"))).thenReturn(Future.successful(("errored", Some("something bad happened"))))

        new AddressLookupCheckStatusLambdaFunction().checkLookupViewStatus(mockRepo, "10")
            .map( result => result shouldBe Map("status" -> "errored", "errorMessage" -> "something bad happened"))
      }
    }
  }
}
