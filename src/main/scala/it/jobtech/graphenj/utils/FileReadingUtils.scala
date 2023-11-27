package it.jobtech.graphenj.utils

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.utils.JtError.JtReadingError

import scala.io.Source

object FileReadingUtils extends LazyLogging {

  def readFile(filePath: String): Either[JtReadingError, String] = {
    logger.info("Load file: %s".format(filePath))
    try ScalaUtils.using(Source.fromFile(filePath, "UTF-8"))(out => Right(out.mkString.trim))
    catch { case t: Throwable => Left(JtReadingError(filePath, t)) }
  }
}
