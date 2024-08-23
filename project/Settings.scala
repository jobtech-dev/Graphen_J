import Dependencies.jtSparkVersion
import sbt.Keys.*
import sbt.{ url, ScmInfo }

object Settings {

  lazy val sonatypeUsername: String           = sys.env.getOrElse("SONATYPE_USERNAME", "")
  lazy val sonatypePassword: String           = sys.env.getOrElse("SONATYPE_PASSWORD", "")
  lazy val gpgPassphrase: Option[Array[Char]] = sys.env.get("PGP_PASSPHRASE").map(x => x.toCharArray)
  lazy val pgpSecretFilePath: String          = sys.env.getOrElse("PGP_SECRET_RING_PATH", "")

  lazy val projectSettings = Seq(
    organization         := "it.jobtech",
    organizationName     := "jobtech",
    organizationHomepage := Some(url("https://jobtech.it/")),
    homepage             := Some(url("https://github.com/jobtech-dev/Graphen_J")),
    description          := "Framework to perform EL, ETL and Data Quality processes for large datasets",
    scalaVersion         := "2.12.17",
    versionScheme        := Some("early-semver"),
    scmInfo              := Some(
      ScmInfo(
        url("https://github.com/jobtech-dev/Graphen_J"),
        "scm:git@github.com:jobtech-dev/Graphen_J"
      )
    ),
    version              := s"0.2.0-spark-${jtSparkVersion.split("\\.").take(2).mkString(".")}-alpha"
  )

}
