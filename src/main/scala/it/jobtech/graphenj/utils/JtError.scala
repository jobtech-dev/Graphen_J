package it.jobtech.graphenj.utils

import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail
import it.jobtech.graphenj.configuration.model.{ DestinationDetail, JtSession, SourceDetail }
import it.jobtech.graphenj.core.models.BookmarkEntryAndStorage

sealed abstract class JtError(val message: String, val cause: Throwable)
    extends Exception(message, cause)
    with Product
    with Serializable

sealed abstract class JtParsingError(
  content: String,
  throwable: Throwable
) extends JtError(s"Unable to parse the content: $content", throwable)

object JtError {

  final case class JtSessionError(
    session: JtSession,
    throwable: Throwable
  ) extends JtError(message = s"Unable to create session: $session", cause = throwable)

  final case class JtReadingError(
    filePath: String,
    throwable: Throwable
  ) extends JtError(s"Unable to read file at:$filePath", throwable)

  final case class JtYamlParsingError(
    yaml: String,
    throwable: io.circe.Error
  ) extends JtParsingError(s"Unable to parse yaml content: $yaml", throwable)

  final case class JtJsonParsingError(
    json: String,
    throwable: io.circe.Error
  ) extends JtError(s"Unable to parse the json content: $json", throwable)

  final case class JtConfigurationFileFormatError(
    fileName: String,
    throwable: Throwable
  ) extends JtParsingError(s"Configuration file extension not supported: $fileName", throwable)

  final case class ReadSourceError(
    source: SourceDetail,
    throwable: Throwable
  ) extends JtError(message = s"Unable to read source: $source", cause = throwable)

  final case class WriteError(
    dest: DestinationDetail,
    throwable: Throwable
  ) extends JtError(message = s"Unable to write data to the destination: $dest", cause = throwable)

  final case class WriteIntegrationError(
    dest: DestinationDetail,
    throwable: Throwable
  ) extends JtError(message = s"Unable to write data to the destination: $dest", cause = throwable)

  final case class BookmarkReadError(
    bookmarkEntry: String,
    storageDetail: JtBookmarkStorageDetail,
    throwable: Throwable
  ) extends JtError(
        message = s"Unable to write the bookmark value for the entry $bookmarkEntry to the destination: $storageDetail",
        cause = throwable
      )

  final case class BookmarkWriteError(
    bookmarkEntry: String,
    dest: JtBookmarkStorageDetail,
    throwable: Throwable
  ) extends JtError(
        message = s"Unable to write the bookmark value for the entry $bookmarkEntry to the destination: $dest",
        cause = throwable
      )

  final case class BookmarkDeleteError(
    bookmarkEntry: String,
    dest: JtBookmarkStorageDetail,
    throwable: Throwable
  ) extends JtError(
        message = s"Unable to delete the bookmark value for the entry $bookmarkEntry to the destination: $dest",
        cause = throwable
      )

  final case class MultipleBookmarksDeleteError(
    bookmarkEntriesAndStorages: Seq[BookmarkEntryAndStorage],
    override val message: String,
    throwable: Throwable
  ) extends JtError(message = message, cause = throwable)

  final case class BookmarkConfigurationError(
    throwable: Throwable
  ) extends JtError(message = s"Unable to write bookmark", cause = throwable)

  final case class StrategyError(
    throwable: Throwable
  ) extends JtError(message = s"An error occurred during the strategy execution", cause = throwable)

}
