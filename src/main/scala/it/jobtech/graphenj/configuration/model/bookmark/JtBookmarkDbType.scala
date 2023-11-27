package it.jobtech.graphenj.configuration.model.bookmark

sealed trait JtBookmarkDbType {

  /** This method returns the class driver (e.g. com.mysql.jdbc.Driver)
    *
    * @return
    *   String
    */
  def getDriver: String

}

object MySQL extends JtBookmarkDbType {
  val MySQL          = "MySQL"
  private val driver = "com.mysql.jdbc.Driver"

  override def getDriver: String = driver
  override def toString: String  = MySQL
}

object PostgreSQL extends JtBookmarkDbType {
  val POSTGRESQL     = "PostgreSQL"
  private val driver = "org.postgresql.Driver"

  override def getDriver: String = driver
  override def toString: String  = POSTGRESQL
}

// just for tests
object SqlLiteDbType extends JtBookmarkDbType {
  val SqlLite        = "SqlLite"
  private val driver = "org.sqlite.JDBC"

  override def getDriver: String = driver
  override def toString: String  = SqlLite
}
