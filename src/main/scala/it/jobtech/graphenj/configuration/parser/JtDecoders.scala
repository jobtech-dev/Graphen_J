package it.jobtech.graphenj.configuration.parser

import cats.implicits.catsSyntaxEitherId
import cats.syntax.functor._
import io.circe.generic.auto._
import io.circe.{ Decoder, DecodingFailure, HCursor, KeyDecoder }
import it.jobtech.graphenj.configuration.model.ApacheIcebergCatalog.{
  GlueApacheIcebergCatalog,
  HadoopApacheIcebergCatalog
}
import it.jobtech.graphenj.configuration.model.SessionType.{ Distributed, Local }
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark._
import org.apache.spark.sql.SaveMode

trait JtDecoders {

  implicit def jtJobTypeDecoder: Decoder[JtJobType] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toLowerCase() match {
          case "etl"    => ETL
          case "dq"     => DQ
          case "etl_dq" => ETL_DQ
        }
      })

  implicit def jtConfigurationDecoder: Decoder[JtJobConfiguration] =
    List[Decoder[JtJobConfiguration]](
      Decoder[JtJobConfiguration.JtEtlDQJobConfiguration].widen,
      Decoder[JtJobConfiguration.JtDQJobConfiguration].widen,
      Decoder[JtJobConfiguration.JtEtlJobConfiguration].widen
    ).reduceLeft(_ or _)

  implicit def seqDecoder[T: Decoder]: Decoder[Seq[T]] =
    Decoder.decodeOption(Decoder.decodeSeq[T]).map(_.toSeq.flatten)

  implicit def mapDecoder[K: KeyDecoder, V: Decoder]: Decoder[Map[K, V]] =
    Decoder.decodeOption(Decoder.decodeMap[K, V]).map(_.getOrElse(Map.empty))

  implicit def sessionTypeDecoder: Decoder[SessionType] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toLowerCase() match {
          case "local"       => Local
          case "distributed" => Distributed
        }
      })

  implicit def saveModeDecoder: Decoder[SaveMode] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toLowerCase() match {
          case "append"        => SaveMode.Append
          case "overwrite"     => SaveMode.Overwrite
          case "errorifexists" => SaveMode.ErrorIfExists
          case "ignore"        => SaveMode.Ignore
        }
      })

  implicit def sourceDetailDecoder: Decoder[SourceDetail] =
    List[Decoder[SourceDetail]](Decoder[SourceDetail.Format].widen, Decoder[SourceDetail.SparkTable].widen)
      .reduceLeft(_ or _)

  implicit def destinationDetailDecoder: Decoder[DestinationDetail] =
    List[Decoder[DestinationDetail]](
      Decoder[DestinationDetail.SparkTable].widen,
      Decoder[DestinationDetail.Format].widen
    ).reduceLeft(_ or _)

  // ApacheIcebergCatalog Decoder

  implicit def apacheIcebergCatalogDecoder: Decoder[ApacheIcebergCatalog] = (cursor: HCursor) =>
    for {
      tpe    <- cursor.get[String]("catalogType")
      result <-
        tpe.toLowerCase match {
          case Hadoop.HADOOP => cursor.as[HadoopApacheIcebergCatalog]
          case Glue.GLUE     => cursor.as[GlueApacheIcebergCatalog]
          case s             => DecodingFailure(s"Invalid house type $s", cursor.history).asLeft
        }
    } yield result

  implicit def writingProviderDecoder: Decoder[Provider] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toLowerCase() match {
          case "iceberg" => Iceberg
        }
      })

  implicit def bookmarkStorageDetailDecoder: Decoder[JtBookmarkStorageDetail] =
    List[Decoder[JtBookmarkStorageDetail]](
      Decoder[JtBookmarkStorageDetail.SparkTable].widen,
      Decoder[JtBookmarkStorageDetail.Database].widen
    ).reduceLeft(_ or _)

  implicit def jtBookmarkStorageTypeDecoder: Decoder[JtBookmarkStorageType] = (c: HCursor) =>
    c.as[String].map {
      case FS.FS => FS
      case OS.OS => OS
      case DB.DB => DB
    }

  implicit def bookmarkDbTypeDecoder: Decoder[JtBookmarkFieldType] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toUpperCase() match {
          case JtBookmarkInt.INT   => JtBookmarkInt
          case JtBookmarkLong.LONG => JtBookmarkLong
          case JtBookmarkDate.DATE => JtBookmarkDate
        }
      })

  implicit def bookmarkFieldTypeDecoder: Decoder[JtBookmarkDbType] = (c: HCursor) =>
    c.as[String].map {
      case MySQL.MySQL           => MySQL
      case PostgreSQL.POSTGRESQL => PostgreSQL
    }

  implicit def jtSaveModeDecoder: Decoder[JtSaveMode] = (c: HCursor) =>
    c.as[String]
      .map(value => {
        value.toLowerCase() match {
          case "append"            => Append
          case "appendonlynewrows" => AppendOnlyNewRows
          case "overwrite"         => Overwrite
          case "errorifexists"     => ErrorIfExists
          case "mergeintobyid"     => MergeIntoById
          case "custom"            => Custom
        }
      })

}
