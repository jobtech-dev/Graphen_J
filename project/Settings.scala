import sbt.Keys.{ homepage, organization, scalaVersion, scmInfo, version }
import sbt.{ url, ScmInfo }

object Settings {

  lazy val SPARK_VERSION = "3.1.1"

  lazy val jtSparkVersion: String =
    sys.props.getOrElse("graphenj.spark.version", SPARK_VERSION)

  lazy val sonatypeUsername                   = sys.env.getOrElse("SONATYPE_USERNAME", "")
  lazy val sonatypePassword                   = sys.env.getOrElse("SONATYPE_PASSWORD", "")
  lazy val gpgPassphrase: Option[Array[Char]] = sys.env.get("PGP_PASSPHRASE").map(x => x.toCharArray)
  lazy val pgpSecretFilePath: String          = sys.env.getOrElse("PGP_SECRET_RING_PATH", "")

  lazy val projectSettings = Seq(
    organization := "it.jobtech",
    homepage     := Some(url("https://github.com/jobtech-dev/Graphen_J")),
    scalaVersion := "2.12.17",
    scmInfo      := Some(
      ScmInfo(
        url("https://github.com/jobtech-dev/Graphen_J"),
        "scm:git@github.com:jobtech-dev/Graphen_J"
      )
    ),
    version      := s"0.2.0-spark-${jtSparkVersion.split("\\.").take(2).mkString(".")}-alpha"
  )

}
