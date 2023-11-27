package it.jobtech.graphenj.writer

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.DestinationDetail.SparkTable
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.utils.JtError.WriteError
import it.jobtech.graphenj.utils.SparkUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

class JtSparkTableWriter(tableIntegrationFunction: Option[(DataFrame, SparkTable) => Either[WriteError, Unit]] = None)(
  implicit ss: SparkSession
) extends Writer[SparkTable]
    with LazyLogging {

  private val TEMP_VIEW = "tmpView"

  override def write(
    df: DataFrame,
    destinationDetail: SparkTable
  ): Either[WriteError, Unit] = {
    logger.info(s"Write Spark Table: $destinationDetail")
    if (destinationDetail.mode == Custom && tableIntegrationFunction.isEmpty) {
      Left(
        WriteError(
          destinationDetail,
          new IllegalArgumentException(
            s"Error writing data with $Custom mode: the function tableIntegrationFunction is missing!"
          )
        )
      )
    } else {
      if (
        !SparkUtils.tableExists(destinationDetail.catalogName, destinationDetail.dbName, destinationDetail.tableName)
      ) {
        createOrReplaceTable(df, destinationDetail)
      } else {
        destinationDetail.mode match {
          case ErrorIfExists =>
            Left(
              WriteError(
                destinationDetail,
                new UnsupportedOperationException(
                  s"Error writing data with $ErrorIfExists mode: table already exists!"
                )
              )
            )

          case Overwrite         => createOrReplaceTable(df, destinationDetail)
          case Append            => appendDf(df, destinationDetail)
          case AppendOnlyNewRows => appendOnlyNewRows(df, destinationDetail)
          case MergeIntoById     => mergeIntoById(df, destinationDetail)
          case Custom            => tableIntegrationFunction.get(df, destinationDetail)
        }
      }
    }
  }

  private def appendDf(df: DataFrame, destinationDetail: SparkTable): Either[WriteError, Unit] = {
    val table =
      "%s.%s.%s".format(destinationDetail.catalogName, destinationDetail.dbName, destinationDetail.tableName)
    Try(df.writeTo(table).append()) match {
      case Failure(exception) => Left(WriteError(destinationDetail, exception))
      case Success(_)         => Right(())
    }
  }

  private def createOrReplaceTable(df: DataFrame, destinationDetail: SparkTable): Either[WriteError, Unit] = {

    Try(createTableWriter(df, destinationDetail).createOrReplace()) match {
      case Failure(throwable) => Left(WriteError(destinationDetail, throwable))
      case Success(_)         => Right(())
    }
  }

  private def createTableWriter(df: DataFrame, dest: SparkTable): CreateTableWriter[Row] = {
    dest.partitionKeys.map(x => col(x)) match {
      case Seq()         => writeTo(df, dest)
      case Seq(x)        => writeTo(df.sortWithinPartitions(x), dest).partitionedBy(x)
      case first +: tail =>
        writeTo(df.sortWithinPartitions(first +: tail: _*), dest).partitionedBy(first, tail: _*)
    }
  }

  private def writeTo(df: DataFrame, dest: SparkTable): CreateTableWriter[Row] = {
    val table             = "%s.%s.%s".format(dest.catalogName, dest.dbName, dest.tableName)
    val createTableWriter = (dest.provider match {
      case Some(provider) => df.writeTo(table).using(provider.toString)
      case None           => df.writeTo(table)
    }).options(dest.options)
    dest.tableProperties.toSeq.foldLeft(createTableWriter)((tw, kv) => tw.tableProperty(kv._1, kv._2))
  }

  def mergeInto(df: DataFrame, dest: SparkTable, query: String): Either[WriteError, Unit] = {
    df.createOrReplaceTempView(TEMP_VIEW)
    Try(ss.sql(query)) match {
      case Failure(exception) => Left(WriteError(dest, exception))
      case Success(_)         => Right(())
    }
  }

  private def mergeIntoById(df: DataFrame, dest: SparkTable): Either[WriteError, Unit] = {
    val query = s"MERGE INTO ${dest.tablePath} t " +
      s"USING (SELECT * FROM $TEMP_VIEW) s " +
      s"ON ${buildMergeIntoOnString(dest.idFields)} " +
      "WHEN MATCHED THEN UPDATE SET * " +
      "WHEN NOT MATCHED THEN INSERT *"
    mergeInto(df, dest, query)
  }

  private def appendOnlyNewRows(df: DataFrame, dest: SparkTable): Either[WriteError, Unit] = {
    val query = s"MERGE INTO ${dest.tablePath} t " +
      s"USING (SELECT * FROM $TEMP_VIEW) s " +
      s"ON ${buildMergeIntoOnString(dest.idFields)} " +
      "WHEN NOT MATCHED THEN INSERT *"
    mergeInto(df, dest, query)
  }

  private def buildMergeIntoOnString(idFields: Seq[String]) = {
    @tailrec
    def supp(idFields: Seq[String], result: String): String = {
      idFields match {
        case Seq(field)    => supp(Seq.empty, s"$result t.$field = s.$field")
        case field :: tail => supp(tail, s"$result t.$field = s.$field AND")
        case Nil           => result
      }
    }
    supp(idFields, "")
  }
}
