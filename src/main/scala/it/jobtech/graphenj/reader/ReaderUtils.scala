package it.jobtech.graphenj.reader

import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkDate,
  JtBookmarkInt,
  JtBookmarkLong,
  JtBookmarkStorageDetail
}
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.repositories.JtBookmarkRepository

object ReaderUtils {

  /** This method reads bookmarks saved inside a spark table
    * @param repository:
    *   repository with which access to the bookmarks
    * @param bookmarkOpt:
    *   bookmark options
    * @return
    *   Option[ JtBookmark[_ >: Int with Long with String] ]
    */
  def readBookmarksSparkTable[D <: JtBookmarkStorageDetail](
    repository: JtBookmarkRepository[D],
    bookmarkOpt: JtBookmarkDetail
  ): Option[JtBookmark[_ >: Int with Long with String]] = {
    bookmarkOpt.bookmarkFieldType match {
      case JtBookmarkInt  => repository.getLastBookmark[Int](bookmarkOpt.bookmarkEntry).toTry.get
      case JtBookmarkLong => repository.getLastBookmark[Long](bookmarkOpt.bookmarkEntry).toTry.get
      case JtBookmarkDate => repository.getLastBookmark[String](bookmarkOpt.bookmarkEntry).toTry.get
    }
  }

}
