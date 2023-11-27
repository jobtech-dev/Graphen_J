package it.jobtech.graphenj.reader

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.SourceDetail.SparkTable
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkDate,
  JtBookmarkInt,
  JtBookmarkLong,
  JtBookmarkStorageDetail
}
import it.jobtech.graphenj.repositories.JtBookmarkRepositoryFsOs
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{ col, lit, unix_timestamp }

import scala.util.{ Failure, Success, Try }

class JtTableReader(implicit ss: SparkSession) extends Reader[SparkTable] with LazyLogging {

  private val DATE_PATTERN = "yyyy-MM-dd"

  /** @param source:
    *   source to read of type SparkTable
    * @param maybeBookmarkConf:
    *   bookmark configuration
    * @return
    *   Either[ReadSourceError, DataFrame]
    */
  override def read(
    source: SparkTable,
    maybeBookmarkConf: Option[JtBookmarksConf]
  ): Either[ReadSourceError, DataFrame] = {
    logger.info(s"Reading source $source")
    Try(
      if (maybeBookmarkConf.isDefined) { readUsingBookmarks(source, maybeBookmarkConf.get) }
      else { ss.read.table(source.tablePath) }
    ) match {
      case Failure(exception) => Left(ReadSourceError(source, exception))
      case Success(df)        => Right(df)
    }
  }

  /** This method read data filtering with bookmarks
    * @param source:
    *   source to read
    * @param bookmarkConf:
    *   bookmark configuration
    * @return
    *   DataFrame
    */
  private def readUsingBookmarks(source: SparkTable, bookmarkConf: JtBookmarksConf): DataFrame = {
    val bookmarkDetail  = bookmarkConf.bookmarkDetail
    val bookmarkStorage = bookmarkConf.bookmarkStorage
    Try(bookmarkStorage.detail match {
      case d: JtBookmarkStorageDetail.SparkTable =>
        ReaderUtils.readBookmarksSparkTable(new JtBookmarkRepositoryFsOs(d), bookmarkDetail)
      case _                                     =>
        throw new IllegalArgumentException(s"Not supported bookmark storage type: $bookmarkStorage.storageType")
    }) match {
      case Failure(exception) => throw exception

      case Success(value) =>
        value match {
          case Some(value) =>
            val df = ss.table(source.tablePath)
            bookmarkDetail.bookmarkFieldType match {
              case JtBookmarkInt | JtBookmarkLong                                                  =>
                df.filter(col(bookmarkDetail.bookmarkField) > value.lastRead)
              case JtBookmarkDate if bookmarkDetail.bookmarkFieldFormat.get.contains(DATE_PATTERN) =>
                df.filter(col(bookmarkDetail.bookmarkField) > value.lastRead)
              case JtBookmarkDate                                                                  =>
                df.filter(
                  unix_timestamp(col(bookmarkDetail.bookmarkField), bookmarkDetail.bookmarkFieldFormat.get) >
                    unix_timestamp(lit(value.lastRead), bookmarkDetail.bookmarkFieldFormat.get)
                )
            }
          case None        => ss.table(source.tablePath)
        }
    }
  }

}
