import sbt._

object Dependencies {
  val jacksonVersion = "3.0.3"
  val doobieVersion = "1.0.0-RC11"

  val compile: Seq[ModuleID] = Seq(
    "com.amazonaws"                      % "aws-lambda-java-core" % "1.4.0",
    "me.lamouri"                         % "jcredstash"           % "2.1.1",
    "com.lihaoyi"                       %% "os-lib"               % "0.11.6",
    "tools.jackson.core"                 % "jackson-core"         % jacksonVersion,
    "tools.jackson.core"                 % "jackson-databind"     % jacksonVersion,
    "com.fasterxml.jackson.core"         % "jackson-annotations"  % "2.20",
    "org.tpolecat"                      %% "doobie-core"          % doobieVersion,
    "org.tpolecat"                      %% "doobie-hikari"        % doobieVersion,
    "org.tpolecat"                      %% "doobie-postgres"      % doobieVersion,
    "ch.qos.logback"                     % "logback-core"         % "1.5.21",
    "org.slf4j"                          % "slf4j-simple"         % "2.0.17",
    "jakarta.xml.bind"                   % "jakarta.xml.bind-api" % "4.0.4",
    "org.glassfish.jaxb"                 % "jaxb-runtime"         % "4.0.6"
)

  val test: Seq[ModuleID] = Seq(
    "org.scalatest"                     %% "scalatest"            % "3.2.19" % Test,
    "org.scalatestplus"                 %% "mockito-5-12"         % "3.2.19.0" % Test
  )
}
