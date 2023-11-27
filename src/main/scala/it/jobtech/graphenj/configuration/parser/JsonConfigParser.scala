package it.jobtech.graphenj.configuration.parser

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{ parser, Decoder }
import it.jobtech.graphenj.utils.JtError.JtJsonParsingError

trait JsonConfigParser extends JtDecoders with EnvironmentVariablesParser with LazyLogging {

  /** This method parses a json content into a specific case class
    * @param content:
    *   String content
    * @param decoder:
    *   Decoder[T]
    * @tparam T:
    *   case class instance to derive
    * @return
    *   Either[JtDqJsonParsingError, T]
    */
  def parseJsonConfig[T](content: String)(implicit decoder: Decoder[T]): Either[JtJsonParsingError, T] = {
    logger.info("Parsing json content")
    parser
      .parse(substituteEnvVars(content))
      .flatMap(_.as[T])
      .leftMap(e => JtJsonParsingError(content, e))
  }

}
