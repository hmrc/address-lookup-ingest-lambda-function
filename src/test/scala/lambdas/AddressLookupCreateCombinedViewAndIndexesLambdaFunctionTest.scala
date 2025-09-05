package lambdas

import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar
import repositories.IngestRepository

import scala.concurrent.Future

class AddressLookupCreateCombinedViewAndIndexesLambdaFunctionTest
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar {
  "AddressLookupCreateCombinedViewAndIndexesLambdaFunction" should {
    "return the number of affected rows" when {
      "IngestRepository.createCombinedLookupView is invoked" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.createCombinedLookupView("some-schema-name"))
          .thenReturn(Future.successful((1, 2)))

        new AddressLookupCreateCombinedViewAndIndexesLambdaFunction()
          .createCombinedViewAndIndexesLambdaFunction(
            mockRepo,
            "some-schema-name"
          )
          .map(result => result shouldBe (1, 2))
      }
    }
  }
}
