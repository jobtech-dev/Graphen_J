package it.jobtech.graphenj.configuration.model

/** Session Product Type
  *
  * @param sessionType
  *   the session type
  * @param applicationName
  *   the application name
  * @param conf:
  *   a map of Apache SparkSession configurations
  * @param icebergSupport:
  *   Enable Apache Iceberg
  * @param icebergCatalogs:
  *   Apache Iceberg catalogs configurations
  */
final case class JtSession(
  sessionType: SessionType,
  applicationName: String,
  conf: Option[Map[String, String]] = None,
  icebergSupport: Option[Boolean] = None,
  icebergCatalogs: Option[Seq[ApacheIcebergCatalog]] = None
) {
  if (icebergSupport.isDefined && icebergSupport.get) {
    require(icebergCatalogs.isDefined && icebergCatalogs.get.nonEmpty)
  }
}

object JtSession {

  implicit class SessionOps(session: JtSession) {
    private val LOCAL = "local[*]"

    def master: Option[String] =
      session.sessionType match {
        case SessionType.Local       => Some(LOCAL)
        case SessionType.Distributed => None
      }
  }
}

sealed trait SessionType extends Product with Serializable

object SessionType {

  /** Local Spark session to run Apache Spark in a local mode */
  final case object Local extends SessionType

  /** Distributed Spark session to run Apache Spark on a cluster */
  final case object Distributed extends SessionType
}
