package it.jobtech.graphenj.configuration.model

import org.apache.spark.sql.SaveMode

sealed trait DestinationDetail extends Product with Serializable

object DestinationDetail {

  /** Format Destination based on the spark format writer.
    *
    * @param format:
    *   Kind of format
    * @param options:
    *   Options configuration
    * @param mode:
    *   SaveMode
    * @param partitionKeys:
    *   Seq of columns with which partition the data
    */
  final case class Format(
    format: String,
    options: Map[String, String],
    mode: SaveMode,
    partitionKeys: Seq[String] = Seq.empty
  ) extends DestinationDetail

  /** SparkTable destination based on the spark writeTo writer functionality.
    * @param catalogName:
    *   catalog name
    * @param dbName:
    *   database name
    * @param tableName:
    *   table name
    * @param mode:
    *   saving mode
    * @param provider:
    *   provider
    * @param options:
    *   Options configuration
    * @param partitionKeys:
    *   Seq of columns with which partition the data
    */
  final case class SparkTable(
    catalogName: String,
    dbName: String,
    tableName: String,
    mode: JtSaveMode,
    integrationTableFunctionClass: Option[String],
    idFields: Seq[String] = Seq.empty,
    provider: Option[Provider] = None,
    tableProperties: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty,
    partitionKeys: Seq[String] = Seq.empty
  ) extends DestinationDetail {
    if (mode == MergeIntoById) { require(idFields.nonEmpty) }
    else if (mode == Custom) { require(integrationTableFunctionClass.nonEmpty) }
    val tablePath: String = "%s.%s.%s".format(catalogName, dbName, tableName)
  }

}

final case class JtDestination(id: String, detail: DestinationDetail)
