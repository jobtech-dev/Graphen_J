package it.jobtech.graphenj.configuration.model

import it.jobtech.graphenj.configuration.model.bookmark.{ JtBookmarkDate, JtBookmarkFieldType, JtBookmarkStorage }

sealed trait SourceDetail extends Product with Serializable

object SourceDetail {

  /** A Format SourceDetail model. Based on the spark format reader.
    *
    * @param format:
    *   file format
    * @param schema:
    *   optional reading schema
    * @param options:
    *   spark reading options
    */
  final case class Format(
    format: String,
    schema: Option[String] = None,
    options: Map[String, String] = Map.empty
  ) extends SourceDetail

  /** A SparkTable SourceDetail model. Based on the spark table reader.
    *
    * @param catalogName:
    *   catalog name
    * @param dbName:
    *   database name
    * @param tableName:
    *   table name
    */
  final case class SparkTable(
    catalogName: String,
    dbName: String,
    tableName: String,
    provider: Option[Provider]
  ) extends SourceDetail {
    val tablePath: String = "%s.%s.%s".format(catalogName, dbName, tableName)
  }

}

/** @param bookmarkEntry:
  *   entry in the bookmark table
  * @param bookmarkField:
  *   field to use as an anchor
  * @param bookmarkFieldType:
  *   type of the bookmarkField value
  * @param bookmarkFieldFormat:
  *   format of the bookmarkField (it's required just for some types)
  */
final case class JtBookmarkDetail(
  bookmarkEntry: String,
  bookmarkField: String,
  bookmarkFieldType: JtBookmarkFieldType,
  bookmarkFieldFormat: Option[String] // Option because it could be useful just for some types
) {
  if (bookmarkFieldType == JtBookmarkDate) { require(bookmarkFieldFormat.isDefined) }
}

/** @param bookmarkDetail:
  *   details to use the bookmarks
  * @param bookmarkStorage:
  *   storage where save/read bookmarks
  */
final case class JtBookmarksConf(bookmarkDetail: JtBookmarkDetail, bookmarkStorage: JtBookmarkStorage)

/** @param id:
  *   id of the source
  * @param detail:
  *   detail to read and have access to the source
  * @param bookmarkConf:
  *   bookmark configuration
  */
final case class JtSource(
  id: String,
  detail: SourceDetail,
  bookmarkConf: Option[JtBookmarksConf] = None
)
