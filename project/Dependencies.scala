import sbt._

object Dependencies {
  val jacksonVersion = "2.9.7"
  val doobieVersion = "0.7.1"

  val compile: Seq[ModuleID] = Seq(
    "com.amazonaws"                      % "aws-lambda-java-core" % "1.2.3",
    "me.lamouri"                         % "jcredstash"           % "2.1.1",
    "com.lihaoyi"                       %% "os-lib"               % "0.7.1",
    "com.fasterxml.jackson.core"         % "jackson-core"         % jacksonVersion,
    "com.fasterxml.jackson.core"         % "jackson-databind"     % jacksonVersion,
    "com.fasterxml.jackson.core"         % "jackson-annotations"  % jacksonVersion,
    "org.tpolecat"                      %% "doobie-core"          % doobieVersion,
    "org.tpolecat"                      %% "doobie-hikari"        % doobieVersion,
    "org.tpolecat"                      %% "doobie-postgres"      % doobieVersion,
    "ch.qos.logback"                     % "logback-core"         % "1.2.3",
    "org.slf4j"                          % "slf4j-simple"         % "1.7.30",
    "javax.xml.bind"                     % "jaxb-api"             % "2.3.1",
    "org.glassfish.jaxb"                 % "jaxb-runtime"         % "2.3.1"
)

  val test: Seq[ModuleID] = Seq(
    "org.scalatest"         %% "scalatest"          % "3.2.19"   % Test,
    "org.scalatestplus"      % "mockito-3-4_2.12"   % "3.2.10.0" % Test
  )
}
