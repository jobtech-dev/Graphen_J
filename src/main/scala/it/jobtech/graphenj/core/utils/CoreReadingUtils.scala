package it.jobtech.graphenj.core.utils

import it.jobtech.graphenj.configuration.model.JtSource
import it.jobtech.graphenj.reader.JtSourceReader
import org.apache.spark.sql.DataFrame

object CoreReadingUtils {

  def readAllSources(sources: Seq[JtSource], jtSourceReader: JtSourceReader): Map[String, DataFrame] = {
    sources
      .map(s =>
        jtSourceReader.read(s.detail, s.bookmarkConf) match {
          case Left(exception) => throw exception
          case Right(value)    => s.id -> value
        }
      )
      .toMap
  }

}
