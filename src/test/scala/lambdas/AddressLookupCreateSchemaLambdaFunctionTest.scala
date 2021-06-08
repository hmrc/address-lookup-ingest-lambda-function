package lambdas

import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar
import repositories.IngestRepository

import scala.concurrent.Future

class AddressLookupCreateSchemaLambdaFunctionTest extends AsyncWordSpec with Matchers with MockitoSugar {
  "AddressLookupCreateSchemaLambdaFunction" should {
    "return the name of the new schema" when {
      "db schema is initialised" in {
        val mockRepo = mock[IngestRepository]
        when(mockRepo.initialiseSchema("10")).thenReturn(Future.successful("ab10_20210608_101010"))

        new AddressLookupCreateSchemaLambdaFunction().initialiseDbSchema(mockRepo, "10")
            .map(result => result shouldBe "ab10_20210608_101010")
      }
    }
  }
}
