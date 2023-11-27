package it.jobtech.graphenj.configuration.parser

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.yaml.parser
import it.jobtech.graphenj.utils.JtError.JtYamlParsingError

trait YamlConfigParser extends JtDecoders with EnvironmentVariablesParser with LazyLogging {

  /** This method parses a yaml content into a specific case class
    * @param content:
    *   String content
    * @param decoder:
    *   Decoder[T]
    * @tparam T:
    *   case class instance to derive
    * @return
    *   Either[JtYamlParsingError, T]
    */
  def parseYamlConfig[T](content: String)(implicit decoder: Decoder[T]): Either[JtYamlParsingError, T] = {
    logger.info("Parsing yaml content")
    parser
      .parse(substituteEnvVars(content))
      .flatMap(_.as[T])
      .leftMap(e => JtYamlParsingError(content, e))
  }
}
