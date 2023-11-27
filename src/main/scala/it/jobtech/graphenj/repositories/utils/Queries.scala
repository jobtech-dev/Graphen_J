package it.jobtech.graphenj.repositories.utils

import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.models.JtBookmark

object Queries {

  def deleteLastBookmarkQuery(table: String, entry: String): String = {
    s"""DELETE FROM ${table}
       |WHERE ${nameOf[JtBookmark[_]](_.entry)} = '$entry' AND
       |${nameOf[JtBookmark[_]](_.timeMillis)}=(SELECT max(${nameOf[JtBookmark[_]](_.timeMillis)})
       |              FROM ${table}
       |              WHERE ${nameOf[JtBookmark[_]](_.entry)}='$entry')""".stripMargin
  }

}
