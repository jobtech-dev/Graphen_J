package it.jobtech.graphenj.configuration.model.bookmark

sealed trait JtBookmarkFieldType

object JtBookmarkInt extends JtBookmarkFieldType {
  val INT                       = "INT"
  override def toString: String = INT
}

object JtBookmarkLong extends JtBookmarkFieldType {
  val LONG                      = "LONG"
  override def toString: String = LONG
}

object JtBookmarkDate extends JtBookmarkFieldType {
  val DATE                      = "DATE"
  override def toString: String = DATE
}
