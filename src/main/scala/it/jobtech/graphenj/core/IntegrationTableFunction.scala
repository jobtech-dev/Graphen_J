package it.jobtech.graphenj.core

import it.jobtech.graphenj.configuration.model.DestinationDetail.SparkTable
import it.jobtech.graphenj.utils.JtError.WriteError
import org.apache.spark.sql.{ DataFrame, SparkSession }

abstract class IntegrationTableFunction(implicit val ss: SparkSession) {
  def function(): (DataFrame, SparkTable) => Either[WriteError, Unit]
}
