package it.jobtech.graphenj.repositories

import io.circe.{ Decoder, Encoder }
import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.JtError

import scala.reflect.runtime.universe._

abstract class JtBookmarkRepository[+D <: JtBookmarkStorageDetail](storageDetail: D) {

  /** This method have to return the last bookmark value of the bookmarkEntry if exists
    * @param bookmarkEntry:
    *   bookmark entry to find
    * @tparam T:
    *   type of the value of the bookmark
    * @return
    *   Option[ JtBookmark[T] ]
    */
  def getLastBookmark[T: TypeTag](bookmarkEntry: String)(implicit
    tt: TypeTag[JtBookmark[T]],
    decoder: Decoder[T]
  ): Either[JtError.BookmarkReadError, Option[JtBookmark[T]]]

  /** This method have to write the bookmark value for the bookmarkEntry in the bookmark table
    * @param bookmarkEntry:
    *   bookmark entry
    * @param value:
    *   value of the bookmark
    * @tparam T:
    *   type of the bookmark value
    * @return
    *   Either[JtError.BookmarkWriteError, Unit]
    */
  def writeBookmark[T: TypeTag](bookmarkEntry: String, value: T)(implicit
    tt: TypeTag[JtBookmark[T]],
    encoder: Encoder[T]
  ): Either[JtError.BookmarkWriteError, Unit]

  /** This method have to delete the last saved bookmark for the specified entry
    * @param bookmarkEntry:
    *   bookmark entry
    * @return
    *   Either[JtError.BookmarkWriteError, Unit]
    */
  def deleteLastBookmark(bookmarkEntry: String): Either[JtError.BookmarkDeleteError, Unit]
}
