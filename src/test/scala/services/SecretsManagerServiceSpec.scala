package services

import com.amazonaws.secretsmanager.caching.SecretCache
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar


class SecretsManagerServiceSpec extends AsyncWordSpec with Matchers with MockitoSugar {

  "SecretsManagerService" should {
    val secretName = "validSecretName"
    val secretKey = "secret"

    "return the secret value when secretName is valid" in {
      val secretString = """{"secret": "mySecretValue"}"""

      val mockSecretCache = mock[SecretCache]
      when(mockSecretCache.getSecretString(secretName)).thenReturn(secretString)

      val service = new SecretsManagerService(mockSecretCache)

      val result = service.getSecret(secretName, secretKey)
      result shouldEqual "mySecretValue"
    }

    "throw an exception when secret string is not a valid JSON" in {
      val secretString = """invalidJson"""

      val mockSecretCache = mock[SecretCache]
      when(mockSecretCache.getSecretString(secretName)).thenReturn(secretString)

      val service = new SecretsManagerService(mockSecretCache)

      an[Exception] should be thrownBy service.getSecret(secretName, secretKey)
    }

    "throw an exception when secret key is missing in JSON" in {
      val secretString = """{"someOtherKey": "mySecretValue"}"""

      val mockSecretCache = mock[SecretCache]
      when(mockSecretCache.getSecretString(secretName)).thenReturn(secretString)

      val service = new SecretsManagerService(mockSecretCache)

      an[NoSuchElementException] should be thrownBy service.getSecret(secretName, secretKey)
    }
  }
}
