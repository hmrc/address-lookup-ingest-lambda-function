import sbt.*

object Dependencies {
  val jacksonVersion = "3.0.3"
  val doobieVersion = "1.0.0-RC11"

  val compile: Seq[ModuleID] = Seq(
    "com.amazonaws"                      % "aws-lambda-java-core" % "1.4.0",
    "com.amazonaws.secretsmanager"       % "aws-secretsmanager-caching-java"  % "2.1.0",
    "me.lamouri"                         % "jcredstash"           % "2.1.1",
    "org.playframework"                 %% "play-json"            % "3.0.6",
    "com.lihaoyi"                       %% "os-lib"               % "0.11.6",
    "org.tpolecat"                      %% "doobie-core"          % doobieVersion,
    "org.tpolecat"                      %% "doobie-hikari"        % doobieVersion,
    "org.tpolecat"                      %% "doobie-postgres"      % doobieVersion,
    "ch.qos.logback"                     % "logback-core"         % "1.5.21",
    "org.slf4j"                          % "slf4j-simple"         % "2.0.17",

    //Legacy JAXB dependencies required for Java 11+ compatibility:
    //  These are AWS SDK v1 dependency that are transitively required for jcredstash
    //   We will move away from jcredstash in future so these can be removed, and use Secrets Manager instead
    "javax.xml.bind"                     % "jaxb-api"             % "2.3.1",
    "org.glassfish.jaxb"                 % "jaxb-runtime"         % "2.3.1"
  )

  val test: Seq[ModuleID] = Seq(
    "org.scalatest"                     %% "scalatest"            % "3.2.19" % Test,
    "org.scalatestplus"                 %% "mockito-5-12"         % "3.2.19.0" % Test
  )
}
