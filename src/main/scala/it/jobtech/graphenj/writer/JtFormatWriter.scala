package it.jobtech.graphenj.writer

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.DestinationDetail.Format
import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.utils.JtError.WriteError
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }

class JtFormatWriter extends Writer[Format] with LazyLogging {

  override def write(df: DataFrame, destinationDetail: Format): Either[JtError.WriteError, Unit] = {
    logger.info(s"Write Format: $destinationDetail")
    val dfWriter =
      df.write.format(destinationDetail.format).options(destinationDetail.options).mode(destinationDetail.mode)
    Try(if (destinationDetail.partitionKeys.isEmpty) { dfWriter.save() }
    else { dfWriter.partitionBy(destinationDetail.partitionKeys: _*).save() }) match {
      case Failure(throwable) => Left(WriteError(destinationDetail, throwable))
      case Success(_)         => Right(())
    }
  }
}
