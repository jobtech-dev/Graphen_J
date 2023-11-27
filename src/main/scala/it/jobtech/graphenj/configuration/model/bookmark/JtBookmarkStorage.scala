package it.jobtech.graphenj.configuration.model.bookmark

import it.jobtech.graphenj.configuration.model.Provider

trait JtBookmarkStorageDetail

object JtBookmarkStorageDetail {

  /** SparkTable Bookmark storage configurations
    * @param catalogName:
    *   catalog name
    * @param dbName:
    *   database name
    * @param tableName:
    *   table name
    * @param provider:
    *   provider
    * @param options:
    *   Options configuration
    */
  final case class SparkTable(
    catalogName: String,
    dbName: String,
    tableName: String,
    provider: Option[Provider],
    tableProperties: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  ) extends JtBookmarkStorageDetail {
    val tablePath: String = "%s.%s.%s".format(catalogName, dbName, tableName)
  }

  /** Jdbc Bookmark storage configurations
    * @param host:
    *   database host (e.g. jdbc:mysql://0.0.0.0:1234)
    * @param user:
    *   username to authenticate
    * @param password:
    *   password to authenticate
    * @param dbName:
    *   database name
    * @param tableName:
    *   bookmark table name
    * @param dbType:
    *   db type
    */
  final case class Database(
    host: String,
    user: String,
    password: String,
    dbName: String,
    tableName: String,
    dbType: JtBookmarkDbType
  ) extends JtBookmarkStorageDetail
}

/** Bookmarks storage configuration
  * @param id:
  *   id of the storage
  * @param storageType:
  *   storage type
  * @param detail:
  *   details to connect to the storage
  */
final case class JtBookmarkStorage(
  id: String,
  storageType: JtBookmarkStorageType,
  detail: JtBookmarkStorageDetail
) {
  storageType match {
    case FS | OS => require(detail.isInstanceOf[JtBookmarkStorageDetail.SparkTable])
    case DB      => require(detail.isInstanceOf[JtBookmarkStorageDetail.Database])
  }
}
