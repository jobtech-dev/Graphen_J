package it.jobtech.graphenj.configuration.model

import it.jobtech.graphenj.configuration.model.JtJobConfiguration.{
  JtDQJobConfiguration,
  JtEtlDQJobConfiguration,
  JtEtlJobConfiguration
}

/** Configuration for the framework
  *
  * @param jobType:
  *   job type you want to perform
  * @param session:
  *   configuration to define the Spark Session
  * @param jobConfiguration:
  *   configuration for the job
  */
case class JtConfiguration(jobType: JtJobType, session: JtSession, jobConfiguration: JtJobConfiguration) {
  jobType match {
    case DQ     => require(jobConfiguration.isInstanceOf[JtDQJobConfiguration])
    case ETL    => require(jobConfiguration.isInstanceOf[JtEtlJobConfiguration])
    case ETL_DQ => require(jobConfiguration.isInstanceOf[JtEtlDQJobConfiguration])
  }
}
