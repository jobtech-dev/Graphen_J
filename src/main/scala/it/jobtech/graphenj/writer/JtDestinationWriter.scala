package it.jobtech.graphenj.writer

import it.jobtech.graphenj.configuration.model.DestinationDetail.{ Format, SparkTable }
import it.jobtech.graphenj.configuration.model.JtDestination
import it.jobtech.graphenj.core.IntegrationTableFunction
import it.jobtech.graphenj.utils.JtError.WriteError
import org.apache.spark.sql.{ DataFrame, SparkSession }

class JtDestinationWriter(integrationTableFunction: Option[IntegrationTableFunction])(implicit ss: SparkSession) {

  private lazy val formatWriter     = new JtFormatWriter()
  private lazy val sparkTableWriter = new JtSparkTableWriter(integrationTableFunction.map(_.function()))

  def write(df: DataFrame, jtDestination: JtDestination): Either[WriteError, Unit] = {
    (jtDestination.id, jtDestination.detail) match {
      case (_, destination: Format)      => formatWriter.write(df, destination)
      case (id, destination: SparkTable) => sparkTableWriter.write(df, destination)
    }
  }

}
