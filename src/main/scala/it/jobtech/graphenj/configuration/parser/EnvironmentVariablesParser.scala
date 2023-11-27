package it.jobtech.graphenj.configuration.parser

trait EnvironmentVariablesParser {

  private val regex = "\\$\\{([A-Za-z0-9\\_\\-]+)\\}".r

  def substituteEnvVars(conf: String): String = {
    regex.replaceAllIn(conf, x => System.getProperty(x.group(1)))
  }

}
