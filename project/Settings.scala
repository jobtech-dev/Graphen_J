import sbt.Def
import sbt.Keys.{ organization, scalaVersion, version }

object Settings {

  lazy val SPARK_311 = "3.1.1"

  lazy val jtSparkVersion: String =
    sys.props.getOrElse("graphenj.spark.version", SPARK_311)

  lazy val projectSettings: Seq[Def.Setting[String]] = Seq(
    organization := "it.jobtech",
    scalaVersion := "2.12.17",
    version      := s"0.2.0-spark-${jtSparkVersion.split("\\.").take(2).mkString(".")}-alpha"
  )

}
