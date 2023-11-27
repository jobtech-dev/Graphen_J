package it.jobtech.graphenj.core.models

import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail
import it.jobtech.graphenj.utils.JtError

case class SavedBookmarkRes(saved: Seq[String], error: Option[JtError])
case class BookmarkEntryAndStorage(bookmarkEntry: String, bookmarkStorage: JtBookmarkStorageDetail)
case class DeletedBookmarksErrors(bookmarks: Seq[BookmarkEntryAndStorage], message: String)
