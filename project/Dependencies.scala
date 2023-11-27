import sbt._

object Dependencies {

  // dependencies versions
  private val icebergVersion              = "1.3.1"
  private val typeSafeScalaLoggingVersion = "3.9.5"
  private val catsVersion                 = "2.8.0"
  private val circeVersion                = "0.14.2"
  private val nameOfVersion               = "4.0.0"
  private val commonLang3Version          = "3.12.0"

  private val sparkIcebergPackage = Settings.jtSparkVersion match {
    case version if version.startsWith("3.3") => "iceberg-spark-runtime-3.3"
    case version if version.startsWith("3.2") => "iceberg-spark-runtime-3.2"
    case version if version.startsWith("3.1") => "iceberg-spark-runtime-3.1"
    case version if version.startsWith("3.0") => "iceberg-spark-runtime-3.0"
    case version                              => throw new IllegalArgumentException(s"Spark version ${version} not supported!")
  }

  private val deequVersion = Settings.jtSparkVersion match {
    case version if version.startsWith("3.3") => "2.0.3-spark-3.3"
    case version if version.startsWith("3.2") => "2.0.1-spark-3.2"
    case version if version.startsWith("3.1") => "2.0.0-spark-3.1"
    case version if version.startsWith("3.0") => "1.2.2-spark-3.0"
    case version                              => throw new IllegalArgumentException(s"Spark version ${version} not supported!")
  }

  // exclusion rules
  lazy val excludeSpark    = ExclusionRule(organization = "org.apache.spark")
  lazy val excludeScalanlp = ExclusionRule(organization = "org.scalanlp")

  // dependencies
  lazy val sparkSql             = "org.apache.spark"           %% "spark-sql"         % Settings.jtSparkVersion % Provided
  lazy val sparkAvro            = "org.apache.spark"           %% "spark-avro"        % Settings.jtSparkVersion % Provided
  lazy val sparkHive            = "org.apache.spark"           %% "spark-hive"        % Settings.jtSparkVersion % Provided
  lazy val sparkIceberg         = "org.apache.iceberg"         %% sparkIcebergPackage % icebergVersion          % Provided
  lazy val typeSafeScalaLogging = "com.typesafe.scala-logging" %% "scala-logging"     % typeSafeScalaLoggingVersion
  lazy val catsCore             = "org.typelevel"              %% "cats-core"         % catsVersion
  lazy val circeCore            = "io.circe"                   %% "circe-core"        % circeVersion
  lazy val circeGeneric         = "io.circe"                   %% "circe-generic"     % circeVersion
  lazy val circeParser          = "io.circe"                   %% "circe-parser"      % circeVersion
  lazy val circeParserYml       = "io.circe"                   %% "circe-yaml"        % circeVersion
  lazy val nameOf               = "com.github.dwickern"        %% "scala-nameof"      % nameOfVersion
  lazy val commonLang3          = "org.apache.commons"          % "commons-lang3"     % commonLang3Version
  lazy val deequ                = "com.amazon.deequ"            % "deequ"             % deequVersion excludeAll (excludeSpark, excludeScalanlp)

  lazy val core_deps: Seq[ModuleID] = Seq(
    sparkSql,
    sparkAvro,
    sparkHive,
    sparkIceberg,
    typeSafeScalaLogging,
    catsCore,
    circeCore,
    circeGeneric,
    circeParser,
    circeParserYml,
    nameOf,
    commonLang3,
    deequ
  )

  // test dependencies versions
  private val scalaTestVersion = "3.2.14"
  private val sqlLiteVersion   = "3.34.0"

  // test dependencies
  lazy val scalatest = "org.scalatest" %% "scalatest"   % scalaTestVersion % Test
  lazy val sqlLite   = "org.xerial"     % "sqlite-jdbc" % sqlLiteVersion   % Test

  lazy val test_deps: Seq[ModuleID] = Seq(scalatest, sqlLite)
}
