package it.jobtech.graphenj.reader

import it.jobtech.graphenj.configuration.model.SourceDetail.{ Format, SparkTable }
import it.jobtech.graphenj.configuration.model.{ JtBookmarksConf, SourceDetail }
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import org.apache.spark.sql.{ DataFrame, SparkSession }

class JtSourceReader(implicit ss: SparkSession) extends Reader[SourceDetail] {

  private lazy val formatReader     = new JtFormatReader()
  private lazy val sparkTableReader = new JtTableReader()

  override def read(
    jtDqSource: SourceDetail,
    maybeBookmarkConf: Option[JtBookmarksConf]
  ): Either[ReadSourceError, DataFrame] = {
    jtDqSource match {
      case source: Format     => formatReader.read(source, maybeBookmarkConf)
      case source: SparkTable => sparkTableReader.read(source, maybeBookmarkConf)
    }
  }
}
