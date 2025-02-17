ThisBuild / version := "1.2"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / assemblyJarName := "address-lookup-file-ingest-lambda-functions_2.12-1.2.jar"
ThisBuild / parallelExecution := false

lazy val lambda = Project("address-lookup-file-ingest-lambda-functions", file("."))
  .settings(
    libraryDependencies ++= Dependencies.compile ++ Dependencies.test
  )
