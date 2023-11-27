package it.jobtech.graphenj.writer

import it.jobtech.graphenj.configuration.model.DestinationDetail
import it.jobtech.graphenj.utils.JtError.WriteError
import org.apache.spark.sql.DataFrame

trait Writer[T <: DestinationDetail] {

  def write(df: DataFrame, destinationDetail: T): Either[WriteError, Unit]

}
