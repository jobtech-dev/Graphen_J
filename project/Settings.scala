import sbt.Keys.{ homepage, organization, scalaVersion, version }
import sbt.url

object Settings {

  lazy val SPARK_VERSION = "3.1.1"

  lazy val jtSparkVersion: String =
    sys.props.getOrElse("graphenj.spark.version", SPARK_VERSION)

  lazy val projectSettings = Seq(
    organization := "it.jobtech",
    homepage     := Some(url("https://github.com/sbt/sbt-ci-release")),
    scalaVersion := "2.12.17"
    // version      := s"0.2.0-spark-${jtSparkVersion.split("\\.").take(2).mkString(".")}-alpha"
  )

}
