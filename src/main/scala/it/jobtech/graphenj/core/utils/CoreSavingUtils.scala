package it.jobtech.graphenj.core.utils

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.github.dwickern.macros.NameOf.nameOf
import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail.{ Database, SparkTable }
import it.jobtech.graphenj.configuration.model.bookmark._
import it.jobtech.graphenj.core.models.{
  BookmarkEntryAndStorage,
  DeequReport,
  DeletedBookmarksErrors,
  SavedBookmarkRes
}
import it.jobtech.graphenj.repositories.{ JtBookmarkRepository, JtBookmarkRepositoryFsOs, JtBookmarkRepositoryJDBC }
import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.utils.JtError.{
  BookmarkConfigurationError,
  BookmarkDeleteError,
  BookmarkWriteError,
  MultipleBookmarksDeleteError
}
import it.jobtech.graphenj.writer.JtDestinationWriter
import org.apache.spark.sql.functions.{ col, lit, unix_timestamp, when }
import org.apache.spark.sql.{ DataFrame, SparkSession }

import java.util.Date
import scala.util.{ Failure, Success, Try }

object CoreSavingUtils extends LazyLogging {

  private val BOOKMARK_DATE_TIMESTAMP = "bookmarkDateTimestamp"
  private val REPORT_TIMESTAMP        = "reportTimestamp"

  /** Given a Seq of JtSource return a Map where the key is the JtSource id and the value is the repository with which
    * write the bookmarks
    * @param sources:
    *   sources to read
    * @param ss:
    *   SparkSession
    * @return
    *   Map[ String, JtBookmarkRepository[JtBookmarkStorageDetail] ]
    */
  def getBookmarkRepositoriesMap(
    sources: Seq[JtSource]
  )(implicit ss: SparkSession): Map[String, JtBookmarkRepository[JtBookmarkStorageDetail]] = {
    sources
      .flatMap(source =>
        source.bookmarkConf.map(storage =>
          storage.bookmarkStorage.storageType match {
            case FS | OS =>
              source.id -> new JtBookmarkRepositoryFsOs(storage.bookmarkStorage.detail.asInstanceOf[SparkTable])
            case DB      =>
              source.id -> new JtBookmarkRepositoryJDBC(storage.bookmarkStorage.detail.asInstanceOf[Database])
            case t       =>
              throw BookmarkConfigurationError(
                new IllegalArgumentException(s"Storage type $t not supported for bookmarks!")
              )
          }
        )
      )
      .toMap
  }

  /** This method save the transformed data and the bookmarks
    * @param sourceDfs:
    *   map where the id is the key of a source and the value is the read dataframe
    * @param transformedDf:
    *   data to save in the destination
    * @param jtDestinationWriter:
    *   writer with which to save data in the destination
    * @param dataDestination:
    *   data destination configuration
    * @param sources:
    *   Seq of sources
    * @param bookmarkRepositoriesMap:
    *   map where the key is the id of the source and the value is the repository with which save the related bookmarks
    * @return
    *   Either[JtError, Unit]
    */
  def saveResultAndBookmarks(
    sourceDfs: Map[String, DataFrame],
    transformedDf: DataFrame,
    jtDestinationWriter: JtDestinationWriter,
    dataDestination: JtDestination,
    sources: Seq[JtSource],
    bookmarkRepositoriesMap: Map[String, JtBookmarkRepository[JtBookmarkStorageDetail]]
  ): Either[JtError, Unit] = {
    saveBookmarks(sourceDfs, sources, bookmarkRepositoriesMap) match {

      case SavedBookmarkRes(savedBookmarks, Some(e)) =>
        logger.error("An error occurred during the bookmarks writing: restoring the initial state!")
        deleteBookmarksAndManageError(sources, bookmarkRepositoriesMap, savedBookmarks, e)

      case SavedBookmarkRes(savedBookmarks, None) =>
        jtDestinationWriter
          .write(transformedDf, dataDestination)
          .left
          .flatMap(e => {
            logger.error("An error occurred during the data writing: restoring the initial state!")
            deleteBookmarksAndManageError(sources, bookmarkRepositoriesMap, savedBookmarks, e)
          })
    }
  }

  private def deleteBookmarksAndManageError(
    sources: Seq[JtSource],
    bookmarkRepositoriesMap: Map[String, JtBookmarkRepository[JtBookmarkStorageDetail]],
    savedBookmarks: Seq[String],
    e: JtError
  ): Left[JtError, Unit] = {
    deleteSavedBookmarks(savedBookmarks, sources, bookmarkRepositoriesMap) match {
      case Nil => Left(e)
      case l   =>
        val res =
          l.foldLeft(DeletedBookmarksErrors(Seq.empty, "Some error occurred during bookmarks deleting!\n"))((res, x) =>
            DeletedBookmarksErrors(
              res.bookmarks :+ BookmarkEntryAndStorage(x.bookmarkEntry, x.dest),
              s"${res.message}${x.getMessage}\n${x.getStackTrace.toSeq}\n"
            )
          )
        Left(MultipleBookmarksDeleteError(res.bookmarks, res.message, new UnsupportedOperationException()))
    }
  }

  private def saveBookmarks(
    sourceDf: Map[String, DataFrame],
    sources: Seq[JtSource],
    bookmarkRepositoriesMap: Map[String, JtBookmarkRepository[JtBookmarkStorageDetail]]
  ): SavedBookmarkRes = {
    sources.foldLeft(SavedBookmarkRes(Seq.empty, None))((res, source) =>
      (res.error, source.bookmarkConf) match {
        case (None, Some(bookmarkConf)) =>
          saveBookmark(bookmarkRepositoriesMap(source.id), sourceDf(source.id), bookmarkConf.bookmarkDetail) match {
            case Left(exception) => SavedBookmarkRes(res.saved, Some(exception))
            case Right(_)        => SavedBookmarkRes(res.saved :+ source.id, res.error)
          }

        case _ => res
      }
    )
  }

  /** This method get the bookmark from the dataframe `df` and save them using the `repository`
    * @param repository:
    *   repository with which to save the bookmarks
    * @param df:
    *   dataframe from which extract the bookmark
    * @param bookmarkOption:
    *   bookmark option
    * @return
    *   Either[BookmarkWriteError, Unit]
    */
  def saveBookmark(
    repository: JtBookmarkRepository[JtBookmarkStorageDetail],
    df: DataFrame,
    bookmarkOption: JtBookmarkDetail
  ): Either[BookmarkWriteError, Unit] = {
    if (df.isEmpty) { return Right(()) }
    bookmarkOption.bookmarkFieldType match {
      case JtBookmarkInt  =>
        repository.writeBookmark(
          bookmarkOption.bookmarkEntry,
          df.sort(col(bookmarkOption.bookmarkField).desc_nulls_last).head().getAs[Int](bookmarkOption.bookmarkField)
        )
      case JtBookmarkLong =>
        repository.writeBookmark(
          bookmarkOption.bookmarkEntry,
          df.sort(col(bookmarkOption.bookmarkField).desc_nulls_last).head().getAs[Long](bookmarkOption.bookmarkField)
        )
      case JtBookmarkDate =>
        val row = df
          .select(col(bookmarkOption.bookmarkField))
          .dropDuplicates()
          .withColumn(
            BOOKMARK_DATE_TIMESTAMP,
            unix_timestamp(col(bookmarkOption.bookmarkField), bookmarkOption.bookmarkFieldFormat.get)
          )
          .sort(col(BOOKMARK_DATE_TIMESTAMP).desc_nulls_last)
          .head()
        repository.writeBookmark(
          bookmarkOption.bookmarkEntry,
          Try(row.getAs[String](bookmarkOption.bookmarkField)) match {
            case Failure(_)     => row.getAs[Date](bookmarkOption.bookmarkField).toString
            case Success(value) => value
          }
        )
    }
  }

  private def deleteSavedBookmarks(
    savedBookmarksSourcesIds: Seq[String],
    sources: Seq[JtSource],
    bookmarkRepositoriesMap: Map[String, JtBookmarkRepository[JtBookmarkStorageDetail]]
  ): Seq[BookmarkDeleteError] = {
    sources
      .filter(x => savedBookmarksSourcesIds.contains(x.id))
      // if a source id is in savedBookmarksSourcesIds, it means that has a bookmarkConf defined
      .flatMap(x =>
        bookmarkRepositoriesMap(x.id).deleteLastBookmark(x.bookmarkConf.get.bookmarkDetail.bookmarkEntry).left.toOption
      )
  }

  /** This method save in the destination, if it's defined, the data quality report
    * @param maybeReportDestination:
    *   destination in which to save the report
    * @param verificationResult:
    *   result of the data quality checking
    * @param writer:
    *   writer with which to save the report
    * @param ss:
    *   SparkSession
    * @return
    *   Option[ Either[JtError.WriteError, Unit] ]
    */
  def saveDqReport(
    maybeReportDestination: Option[JtDestination],
    verificationResult: VerificationResult,
    writer: JtDestinationWriter
  )(implicit ss: SparkSession): Option[Either[JtError.WriteError, Unit]] = {
    maybeReportDestination.map(destination => {
      logger.info("Saving the report..")
      writer
        .write(
          checkResultsAsDataFrame(ss, verificationResult)
            .withColumn(
              nameOf[DeequReport](_.constraint_message),
              when(col(nameOf[DeequReport](_.constraint_message)) === "", null)
                .otherwise(col(nameOf[DeequReport](_.constraint_message)))
            )
            .withColumn(REPORT_TIMESTAMP, lit(System.currentTimeMillis()))
            .repartition(1),
          destination
        )
    })
  }

}
