package it.jobtech.graphenj.application

import it.jobtech.graphenj.configuration.model.DestinationDetail
import it.jobtech.graphenj.configuration.model.DestinationDetail.SparkTable
import it.jobtech.graphenj.core.IntegrationTableFunction
import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.utils.JtError.WriteError
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.util.{ Failure, Success, Try }

class PeopleIntegrationTableFunction(implicit ss: SparkSession) extends IntegrationTableFunction {
  private val catalog   = System.getProperty("DESTINATION_CATALOG")
  private val db        = System.getProperty("DB")
  private val table     = System.getProperty("TABLE")
  private val tablePath = "%s.%s.%s".format(catalog, db, table)

  override def function(): (DataFrame, DestinationDetail.SparkTable) => Either[JtError.WriteError, Unit] = {
    (df: DataFrame, dest: SparkTable) =>
      {
        df.createOrReplaceTempView("my_view")
        Try(ss.sql(sqlText = s"INSERT INTO $tablePath (SELECT * FROM my_view)")) match {
          case Failure(exception) => Left(WriteError(dest, exception))
          case Success(_)         => Right(())
        }
      }
  }
}
