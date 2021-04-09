ThisBuild / name := "address-lookup-file-ingest-lambda-functions"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.12"

ThisBuild / assemblyJarName := "address-lookup-file-ingest-lambda-functions_2.12-1.0.jar"

ThisBuild / parallelExecution := false

val jacksonVersion = "2.9.7"
val doobieVersion = "0.7.1"

ThisBuild / libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-lambda-java-core" % "1.1.0",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.915",
  "com.amazonaws" % "aws-java-sdk-kms" % "1.11.915",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.55",
  "com.amazonaws" % "aws-java-sdk-rds" % "1.11.915",
//  "co.wrisk.jcredstash" % "jcredstash" % "0.0.3",
  "com.lihaoyi" %% "os-lib" % "0.7.1",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)