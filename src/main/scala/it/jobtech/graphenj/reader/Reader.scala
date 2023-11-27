package it.jobtech.graphenj.reader

import it.jobtech.graphenj.configuration.model.{ JtBookmarksConf, SourceDetail }
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import org.apache.spark.sql.DataFrame

trait Reader[T <: SourceDetail] {

  /** @param source:
    *   source to read
    * @param maybeBookmarkConf:
    *   bookmark configurations
    * @return
    *   Either[ReadSourceError, DataFrame]
    */
  def read(source: T, maybeBookmarkConf: Option[JtBookmarksConf]): Either[ReadSourceError, DataFrame]

}
