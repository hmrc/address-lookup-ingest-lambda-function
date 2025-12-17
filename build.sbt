val appVersion = "2.1.0"

ThisBuild / version := appVersion
ThisBuild / scalaVersion := "3.3.7"
ThisBuild / assemblyJarName := s"address-lookup-file-ingest-lambda-functions_3-$appVersion.jar"
ThisBuild / parallelExecution := false

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "FastDoubleParser-LICENSE") => MergeStrategy.first
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("module-info.class")  => MergeStrategy.discard
  case PathList(".gitkeep")           => MergeStrategy.discard
  case x                              => (assembly / assemblyMergeStrategy).value(x) // For all the other files, use the default sbt-assembly merge strategy
}

lazy val lambda = Project("address-lookup-file-ingest-lambda-functions", file("."))
  .settings(
    libraryDependencies ++= Dependencies.compile ++ Dependencies.test
  )
