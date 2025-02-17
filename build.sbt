ThisBuild / version := "1.2"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / assemblyJarName := "address-lookup-file-ingest-lambda-functions_2.12-1.2.jar"
ThisBuild / parallelExecution := false

ThisBuild / assemblyMergeStrategy := {
  case PathList("module-info.class")  => MergeStrategy.discard
  case PathList(".gitkeep")           => MergeStrategy.discard
  case x                              => (assembly / assemblyMergeStrategy).value(x) // For all the other files, use the default sbt-assembly merge strategy
}

lazy val lambda = Project("address-lookup-file-ingest-lambda-functions", file("."))
  .settings(
    libraryDependencies ++= Dependencies.compile ++ Dependencies.test
  )
