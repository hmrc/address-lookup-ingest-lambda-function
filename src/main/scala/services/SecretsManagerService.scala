package services

import com.amazonaws.secretsmanager.caching.SecretCache
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

class SecretsManagerService(secretCache: SecretCache) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getSecret(secretName: String, secretKey: String): String = {
    logger.info(s"Getting secret from secrets manager")

    val secretString = secretCache.getSecretString(secretName)
    val secretAsJSON = Json.parse(secretString)

    secretAsJSON(secretKey).as[String]
  }
}
