package it.jobtech.graphenj.reader

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.SourceDetail.Format
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkDate,
  JtBookmarkInt,
  JtBookmarkLong,
  JtBookmarkStorage,
  JtBookmarkStorageDetail
}
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.repositories.JtBookmarkRepositoryFsOs
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions.{ col, lit, unix_timestamp }
import org.apache.spark.sql.{ DataFrame, SparkSession }

import java.net.URI
import java.util.Date
import scala.util.{ Failure, Success, Try }

class JtFormatReader(implicit ss: SparkSession) extends Reader[Format] with LazyLogging {

  private val EQ   = "="
  private val PATH = "path"

  /** @param source
    *   : source to read
    * @param maybeBookmarkConf
    *   : bookmark configuration
    * @return
    *   Either[ReadSourceError, DataFrame]
    */
  override def read(source: Format, maybeBookmarkConf: Option[JtBookmarksConf]): Either[ReadSourceError, DataFrame] = {
    logger.info(s"Reading source $source")
    Try(
      if (maybeBookmarkConf.isDefined) { readUsingBookmarks(source, maybeBookmarkConf.get) }
      else { readSource(source) }
    ) match {
      case Failure(exception) => Left(ReadSourceError(source, exception))
      case Success(df)        => Right(df)
    }
  }

  /** This method read the source
    * @param source:
    *   source to read
    * @return
    *   DataFrame
    */
  private def readSource(source: Format): DataFrame = {
    source.schema
      .map(x => ss.read.schema(x))
      .getOrElse(ss.read)
      .format(source.format)
      .options(source.options)
      .load()
  }

  /** This method returns an array of all partitions to read
    * @param bookmarkDateFormat:
    *   bookmark date format
    * @param path:
    *   path to read
    * @param bookmarkDate:
    *   bookmark date value
    * @return
    *   Array[String]
    */
  private def getPartitionsToRead(
    bookmarkDateFormat: String,
    path: String,
    bookmarkDate: String
  ): Array[String] = {
    val bookmarkValue = getBookmarkDate(bookmarkDateFormat, bookmarkDate)
    FileSystem
      .get(new URI(path), ss.sparkContext.hadoopConfiguration)
      .listStatus(new Path(path))
      .filter(x => x.isDirectory)
      .flatMap(x => {
        val stringDate = x.getPath.getName.split(EQ).last
        if (getBookmarkDate(bookmarkDateFormat, stringDate).compareTo(bookmarkValue) > 0) { Some(stringDate) }
        else { None }
      })
  }

  /** This method returns the last bookmark if exists
    * @param bookmarkStorage:
    *   bookmark storage
    * @param bookmarkOpt:
    *   bookmark options
    * @return
    *   Try[ Option[ JtBookmark[_ >: Int with Long with String] ] ]
    */
  private def readLastBookmark(
    bookmarkStorage: JtBookmarkStorage,
    bookmarkOpt: JtBookmarkDetail
  ): Try[Option[JtBookmark[_ >: Int with Long with String]]] = {
    Try(bookmarkStorage.detail match {
      case d: JtBookmarkStorageDetail.SparkTable =>
        ReaderUtils.readBookmarksSparkTable(new JtBookmarkRepositoryFsOs(d), bookmarkOpt)
      case _                                     =>
        throw new IllegalArgumentException(s"Not supported bookmark storage type: ${bookmarkStorage.storageType}")
    })
  }

  /** This method reads data using bookmarks
    * @param source:
    *   source to read
    * @param bookmarkConf:
    *   bookmark configurations
    * @return
    *   DataFrame
    */
  private def readUsingBookmarks(source: Format, bookmarkConf: JtBookmarksConf): DataFrame = {
    val bookmarkDetail = bookmarkConf.bookmarkDetail
    readLastBookmark(bookmarkConf.bookmarkStorage, bookmarkDetail) match {
      case Success(maybeBookmark) =>
        maybeBookmark match {
          case Some(bookmark) =>
            bookmarkDetail.bookmarkFieldType match {
              case JtBookmarkInt | JtBookmarkLong =>
                readSource(source).filter(col(bookmarkDetail.bookmarkField) > bookmark.lastRead)
              case JtBookmarkDate                 =>
                source.options.get(PATH) match {
                  // file system or object storage case
                  case Some(path) =>
                    readPartitionsDateUsingPath(source, bookmarkDetail, bookmark, path)
                  // other cases
                  case None       =>
                    readSource(source).filter(
                      unix_timestamp(col(bookmarkDetail.bookmarkField), bookmarkDetail.bookmarkFieldFormat.get) >
                        unix_timestamp(lit(bookmark.lastRead), bookmarkDetail.bookmarkFieldFormat.get)
                    )
                }
            }
          case None           => readSource(source)
        }

      case Failure(exception) => throw exception
    }
  }

  /** This method parses the bookmark (data) and reads partitions with a value greater than it
    * @param source:
    *   source to read
    * @param bookmarkOpt:
    *   bookmark options
    * @param bookmark:
    *   bookmark
    * @param path:
    *   path to read
    * @return
    *   DataFrame
    */
  private def readPartitionsDateUsingPath(
    source: Format,
    bookmarkOpt: JtBookmarkDetail,
    bookmark: JtBookmark[_ >: Int with Long with String],
    path: String
  ): DataFrame = {
    val partitionsToRead = getPartitionsToRead(
      bookmarkOpt.bookmarkFieldFormat.get,
      path,
      bookmark.lastRead.asInstanceOf[String]
    )
    readSource(source).filter(col(bookmarkOpt.bookmarkField).isin(partitionsToRead: _*))
  }

  private def getBookmarkDate(dateFormat: String, stringDate: String): Date = {
    DateUtils.parseDate(stringDate, dateFormat)
  }
}
