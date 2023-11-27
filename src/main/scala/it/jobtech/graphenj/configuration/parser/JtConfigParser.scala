package it.jobtech.graphenj.configuration.parser

import io.circe.generic.auto._
import it.jobtech.graphenj.configuration.model.JtConfiguration
import it.jobtech.graphenj.utils.JtError.JtConfigurationFileFormatError
import it.jobtech.graphenj.utils.{ JtError, ScalaUtils }

import scala.io.Source

trait JtConfigParser extends YamlConfigParser with JsonConfigParser {

  /** This method parses the configuration file. Supported file format are yaml and json
    * @param configFilePath:
    *   path of the configuration file
    * @return
    *   Either[JtError, JtConfiguration]
    */
  def parse(configFilePath: String): Either[JtError, JtConfiguration] = {
    val config = ScalaUtils.using(Source.fromResource(configFilePath))(_.mkString)
    if (configFilePath.endsWith(".json")) {
      parseJsonConfig[JtConfiguration](config)
    } else if (configFilePath.endsWith(".yml") || configFilePath.endsWith(".yaml")) {
      parseYamlConfig[JtConfiguration](config)
    } else {
      Left(
        JtConfigurationFileFormatError(
          configFilePath,
          new UnsupportedOperationException("Configuration file format not supported!")
        )
      )
    }
  }
}
