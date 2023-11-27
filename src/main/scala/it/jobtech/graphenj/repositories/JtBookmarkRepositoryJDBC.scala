package it.jobtech.graphenj.repositories

import io.circe.parser._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }
import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.repositories.utils.Queries
import it.jobtech.graphenj.repositories.utils.RepositoriesImplicits._
import it.jobtech.graphenj.utils.JtError

import java.sql.{ Connection, DriverManager }
import scala.reflect.runtime.universe._
import scala.util.{ Failure, Success, Try }

class JtBookmarkRepositoryJDBC(storageDetail: JtBookmarkStorageDetail.Database)
    extends JtBookmarkRepository[JtBookmarkStorageDetail.Database](storageDetail) {

  private val obj = "obj"

  Class.forName(storageDetail.dbType.getDriver)
  val connection: Connection = DriverManager.getConnection(
    s"${storageDetail.host}/${storageDetail.dbName}",
    storageDetail.user,
    storageDetail.password
  )

  private def tableExists(tableName: String): Boolean = {
    val resultSet = connection.getMetaData.getTables(null, null, tableName, Array("TABLE"));
    resultSet.next();
  }

  override def getLastBookmark[T: TypeTag](bookmarkEntry: String)(implicit
    tt: TypeTag[JtBookmark[T]],
    decoder: Decoder[T]
  ): Either[JtError.BookmarkReadError, Option[JtBookmark[T]]] = {
    Try({
      if (tableExists(storageDetail.tableName)) {
        val sql       =
          s"""SELECT ${obj}
             |FROM ${storageDetail.tableName}
             |WHERE entry = '${bookmarkEntry}'
             |ORDER BY timeMillis DESC
             |limit 1""".stripMargin
        val statement = connection.createStatement()
        statement.execute(sql)
        statement.getResultSet.toStream
          .map(rs => decode[JtBookmark[T]](rs.getString(obj)).toOption)
          .headOption
          .flatten
      } else {
        None
      }
    }) match {
      case Failure(exception) =>
        Left(JtError.BookmarkReadError(bookmarkEntry, storageDetail, exception))
      case Success(value)     => Right(value)
    }
  }

  override def writeBookmark[T: TypeTag](bookmarkEntry: String, value: T)(implicit
    tt: TypeTag[JtBookmark[T]],
    encoder: Encoder[T]
  ): Either[JtError.BookmarkWriteError, Unit] = {
    Try({
      val bookmark = JtBookmark[T](bookmarkEntry, value, System.currentTimeMillis())

      // Create table if it doesn't exist
      val createTableQuery     =
        s"""
           |CREATE TABLE IF NOT EXISTS ${storageDetail.tableName} (
           |entry VARCHAR(255) NOT NULL,
           |timeMillis BIGINT NOT NULL,
           |${obj} TEXT NOT NULL,
           |PRIMARY KEY (entry, timeMillis)
           |);
           |""".stripMargin
      val createTableStatement = connection.createStatement()
      createTableStatement.executeUpdate(createTableQuery)

      // Insert bookmark
      val insertQuery     =
        s"""INSERT OR REPLACE INTO ${storageDetail.tableName} (entry, timeMillis, ${obj})
           |VALUES ('${bookmark.entry}', '${bookmark.timeMillis}', '${bookmark.asJson.noSpaces}')""".stripMargin
      val insertStatement = connection.createStatement()
      insertStatement.executeUpdate(insertQuery)
    }) match {
      case Failure(exception) =>
        Left(JtError.BookmarkWriteError(bookmarkEntry, storageDetail, exception))
      case Success(value)     => Right(())
    }
  }

  override def deleteLastBookmark(bookmarkEntry: String): Either[JtError.BookmarkDeleteError, Unit] = {
    Try({
      if (tableExists(storageDetail.tableName)) {
        connection
          .createStatement()
          .executeUpdate(Queries.deleteLastBookmarkQuery(storageDetail.tableName, bookmarkEntry))
      }
    }) match {
      case Failure(exception) =>
        Left(JtError.BookmarkDeleteError(bookmarkEntry, storageDetail, exception))
      case Success(_)         => Right(())
    }
  }
}
