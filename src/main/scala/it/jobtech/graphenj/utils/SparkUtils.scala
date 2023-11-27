package it.jobtech.graphenj.utils

import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.{ Failure, Success, Try }

object SparkUtils {

  private val TABLE_NAME = "tableName"

  def tableExists(catalogName: String, dbName: String, tableName: String)(implicit ss: SparkSession): Boolean = {
    Try(
      ss.sql("SHOW TABLES IN %s.%s".format(catalogName, dbName))
    ) match {
      case Failure(_: NoSuchNamespaceException) => false
      case Failure(exception)                   => throw exception
      case Success(df)                          => !df.filter(col(TABLE_NAME) === tableName).rdd.isEmpty()
    }
  }

}
