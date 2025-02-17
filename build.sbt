ThisBuild / name := "address-lookup-file-ingest-lambda-functions"
ThisBuild / version := "1.2"
ThisBuild / scalaVersion := "2.12.12"

ThisBuild / assemblyJarName := "address-lookup-file-ingest-lambda-functions_2.12-1.2.jar"

ThisBuild / parallelExecution := false

val jacksonVersion = "2.9.7"
val doobieVersion = "0.7.1"

ThisBuild / libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-lambda-java-core" % "1.1.0",
  "me.lamouri" % "jcredstash" % "2.1.1",
  "com.lihaoyi" %% "os-lib" % "0.7.1",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion,
  "ch.qos.logback" % "logback-core" % "1.2.3",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "org.scalatestplus" % "mockito-3-4_2.12" % "3.1.3.0" % Test,
)