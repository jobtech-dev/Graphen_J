package it.jobtech.graphenj.utils

import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{ CreateTableWriter, DataFrame, Row, SparkSession }

import scala.util.{ Failure, Success, Try }

object TablesFsUtils {

  def tableExists(catalogName: String, dbName: String, tableName: String)(implicit ss: SparkSession): Boolean = {
    Try(
      ss.sql("SHOW TABLES IN %s.%s".format(catalogName, dbName))
    ) match {
      case Failure(_: NoSuchNamespaceException) => false
      case Failure(exception)                   => throw exception
      case Success(df)                          => !df.filter(col("tableName") === tableName).rdd.isEmpty()
    }
  }

  def createIcebergTable(
    icebergTablePath: String,
    df: DataFrame,
    partitionKey: Seq[String],
    replace: Boolean
  ): Try[Unit] = {
    def createTableWriter(icebergTablePath: String, df: DataFrame): CreateTableWriter[Row] = {
      df
        .writeTo(icebergTablePath)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
    }
    def createTable(replace: Boolean, writer: CreateTableWriter[Row]): Unit                = {
      if (replace) { writer.createOrReplace() }
      else { writer.create() }
    }

    val writer = partitionKey.map(x => col(x)) match {
      case Seq()         => createTableWriter(icebergTablePath, df)
      case Seq(x)        => createTableWriter(icebergTablePath, df).partitionedBy(x)
      case first +: tail =>
        createTableWriter(icebergTablePath, df.sortWithinPartitions((first +: tail): _*)).partitionedBy(first, tail: _*)
    }
    Try(createTable(replace, writer))
  }

}
