ThisBuild / name := "address-lookup-file-ingest-lambda-functions"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / assemblyJarName := assemblyName
ThisBuild / parallelExecution := false

val jacksonVersion = "2.9.7"
val doobieVersion = "0.7.1"

ThisBuild / libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-lambda-java-core" % "1.1.0",
  "com.amazonaws" % "aws-java-sdk-rds" % "1.11.915",
  "co.wrisk.jcredstash" % "jcredstash" % "0.0.3",
  "com.lihaoyi" %% "os-lib" % "0.7.1",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.h2database" % "h2" % "1.4.200" % "test, it"
)

def assemblyName = s"${ThisBuild / name}_${ThisBuild / scalaVersion}-${ThisBuild / version}.jar"
