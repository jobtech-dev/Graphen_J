ThisBuild / versionScheme := Some("early-semver")

// the tests must be performed sequentially otherwise there will be problems due to the closing and recreating of the
// spark session in the different tests
Test / parallelExecution := false

lazy val root = (project in file("."))
  .settings(name := "Graphen_J")
  .settings(inThisBuild(Settings.projectSettings))
  .settings(
    scalacOptions ++= Seq(
      "-encoding",
      "utf8",
      "-feature",
      "-language:implicitConversions",
      "-language:existentials",
      "-unchecked",
      "-Xlint"
    )
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys    := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "it.jobtech.graphenj"
  )
  .settings(libraryDependencies ++= Dependencies.core_deps)
  .settings(libraryDependencies ++= Dependencies.test_deps)

enablePlugins(JavaAppPackaging)

assembly / assemblyMergeStrategy   := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

assembly / assemblyShadeRules      := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository                 := "https://s01.oss.sonatype.org/service/local"
