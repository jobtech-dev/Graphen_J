package it.jobtech.graphenj.application

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.application.JtSessionFactory.SparkSessionBuilder
import it.jobtech.graphenj.configuration.model.ApacheIcebergCatalog.{
  GlueApacheIcebergCatalog,
  HadoopApacheIcebergCatalog,
  ICEBERG_CATALOG,
  ICEBERG_SPARK_CATALOG
}
import it.jobtech.graphenj.configuration.model.{ ApacheIcebergCatalog, JtSession }
import it.jobtech.graphenj.utils.JtError.JtSessionError
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS

import scala.util.{ Failure, Success, Try }

class JtSessionFactory extends LazyLogging {

  /** Create a SparkSession
    *
    * @param session
    *   : A JtSession that define how to build a SparkSession
    * @return
    *   Either[JtSessionError, SparkSession]
    */
  def getOrCreate(session: JtSession): Either[JtSessionError, SparkSession] = {
    logger.info("Creating SparkSession as: %s".format(session))
    Try(
      SparkSession
        .builder()
        .appName(session.applicationName)
        .master(session.master)
        .sparkConf(session.conf)
        .icebergSupport(session.icebergSupport)
        .icebergCatalogs(session.icebergCatalogs)
        .getOrCreate()
    ) match {
      case Failure(exception) => Left(JtSessionError(session, exception))
      case Success(value)     => Right(value)
    }
  }
}

object JtSessionFactory {

  def apply(): JtSessionFactory = new JtSessionFactory()

  implicit class SparkSessionBuilder(builder: SparkSession.Builder) extends LazyLogging {
    private val ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

    def master(maybeMaster: Option[String]): SparkSession.Builder =
      maybeMaster.fold(builder) { m =>
        logger.info("Configuring master to: %s".format(m))
        builder.master(m)
      }

    def sparkConf(maybeConf: Option[Map[String, String]]): SparkSession.Builder =
      maybeConf.filter(_.nonEmpty).fold(builder) { configMap =>
        logger.info("Configuring SparkConf on SparkSessionBuilder with config: %s".format(maybeConf))
        builder.config(new SparkConf().setAll(configMap))
      }

    def icebergSupport(maybeIcebergSupport: Option[Boolean]): SparkSession.Builder =
      maybeIcebergSupport.filter(identity).fold(builder) { _ =>
        builder
          .config(SPARK_SESSION_EXTENSIONS.key, ICEBERG_EXTENSION)
          .config("spark.sql.catalog.spark_catalog", ICEBERG_CATALOG)
      }

    def icebergCatalogs(icebergCatalogs: Option[Seq[ApacheIcebergCatalog]]): SparkSession.Builder =
      icebergCatalogs.filter(_.nonEmpty).fold(builder) { catalogs =>
        catalogs.foldLeft(builder) {
          case (ssBuilder, catalog: HadoopApacheIcebergCatalog) =>
            (catalog.s3aEndpoint match {
              case Some(endpoint) =>
                ssBuilder.config(s"spark.sql.catalog.${catalog.catalogName}.hadoop.fs.s3a.endpoint", endpoint)
              case None           => ssBuilder
            })
              .config(s"spark.sql.catalog.${catalog.catalogName}", ICEBERG_SPARK_CATALOG)
              .config(s"spark.sql.catalog.${catalog.catalogName}.type", catalog.catalogType.toString)
              .config(s"spark.sql.catalog.${catalog.catalogName}.warehouse", catalog.warehouse)

          case (ssBuilder, catalog: GlueApacheIcebergCatalog) =>
            ssBuilder
              .config(s"spark.sql.catalog.${catalog.catalogName}", ICEBERG_SPARK_CATALOG)
              .config(s"spark.sql.catalog.${catalog.catalogName}.catalog-impl", catalog.catalogImpl)
              .config(s"spark.sql.catalog.${catalog.catalogName}.warehouse", catalog.warehouse)
        }
      }
  }

}
