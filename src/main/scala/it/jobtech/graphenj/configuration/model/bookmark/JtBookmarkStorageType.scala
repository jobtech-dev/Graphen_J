package it.jobtech.graphenj.configuration.model.bookmark

trait JtBookmarkStorageType

object OS extends JtBookmarkStorageType {
  val OS                        = "OS"
  override def toString: String = OS
}

object FS extends JtBookmarkStorageType {
  val FS                        = "FS"
  override def toString: String = FS
}

object DB extends JtBookmarkStorageType {
  val DB                        = "DB"
  override def toString: String = DB
}
