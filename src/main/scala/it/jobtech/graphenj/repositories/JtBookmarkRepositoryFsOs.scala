package it.jobtech.graphenj.repositories

import com.github.dwickern.macros.NameOf.nameOf
import io.circe.{ Decoder, Encoder }
import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail.SparkTable
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.repositories.utils.Queries
import it.jobtech.graphenj.utils.JtError.{ BookmarkDeleteError, BookmarkReadError, BookmarkWriteError }
import it.jobtech.graphenj.utils.{ JtError, SparkUtils }
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.reflect.runtime.universe._
import scala.util.{ Failure, Success, Try }

class JtBookmarkRepositoryFsOs(storageDetails: SparkTable)(implicit ss: SparkSession)
    extends JtBookmarkRepository(storageDetails) {

  def getLastBookmark[T: TypeTag](bookmarkEntry: String)(implicit
    tt: TypeTag[JtBookmark[T]],
    decoder: Decoder[T]
  ): Either[JtError.BookmarkReadError, Option[JtBookmark[T]]] = {
    Try(if (SparkUtils.tableExists(storageDetails.catalogName, storageDetails.dbName, storageDetails.tableName)) {
      ss.table(storageDetails.tablePath)
        .filter(col(nameOf[JtBookmark[_]](_.entry)) === bookmarkEntry)
        .sort(col(nameOf[JtBookmark[_]](_.timeMillis)).desc)
        .limit(1)
        .collect()
        .headOption
        .map(row =>
          JtBookmark[T](
            row.getAs[String](nameOf[JtBookmark[_]](_.entry)),
            row.getAs[T](nameOf[JtBookmark[_]](_.lastRead)),
            row.getAs[Long](nameOf[JtBookmark[_]](_.timeMillis))
          )
        )
    } else {
      None
    }) match {
      case Failure(exception) => Left(BookmarkReadError(bookmarkEntry, storageDetails, exception))
      case Success(value)     => Right(value)
    }
  }

  private def writeTo(df: DataFrame, dest: SparkTable): CreateTableWriter[Row] = {
    val table             = "%s.%s.%s".format(dest.catalogName, dest.dbName, dest.tableName)
    val createTableWriter = (dest.provider match {
      case Some(provider) => df.writeTo(table).using(provider.toString)
      case None           => df.writeTo(table)
    }).options(dest.options)
    dest.tableProperties.toSeq.foldLeft(createTableWriter)((tw, kv) => tw.tableProperty(kv._1, kv._2))
  }

  override def writeBookmark[T: TypeTag](bookmarkEntry: String, value: T)(implicit
    tt: TypeTag[JtBookmark[T]],
    encoder: Encoder[T]
  ): Either[JtError.BookmarkWriteError, Unit] = {
    val table = s"${storageDetails.catalogName}.${storageDetails.dbName}.${storageDetails.tableName}"
    Try({
      val bookmarkDf = ss.createDataFrame(Seq(JtBookmark[T](bookmarkEntry, value, System.currentTimeMillis)))
      if (SparkUtils.tableExists(storageDetails.catalogName, storageDetails.dbName, storageDetails.tableName)) {
        bookmarkDf.writeTo(table).append()
      } else {
        writeTo(bookmarkDf, storageDetails).create()
      }
    }) match {
      case Failure(exception) => Left(BookmarkWriteError(bookmarkEntry, storageDetails, exception))
      case Success(_)         => Right(())
    }
  }

  override def deleteLastBookmark(bookmarkEntry: String): Either[BookmarkDeleteError, Unit] = {
    Try(ss.sql(Queries.deleteLastBookmarkQuery(storageDetails.tablePath, bookmarkEntry))) match {
      case Failure(exception) => Left(BookmarkDeleteError(bookmarkEntry, storageDetails, exception))
      case Success(_)         => Right(())
    }
  }
}
