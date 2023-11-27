package it.jobtech.graphenj.application

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.{
  JtDQJobConfiguration,
  JtEtlDQJobConfiguration,
  JtEtlJobConfiguration
}
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.parser.JtConfigParser
import it.jobtech.graphenj.core.strategy.{ JtDqStrategy, JtEtlStrategy }
import it.jobtech.graphenj.core.{ IntegrationTableFunction, JtDqJob, JtEtlDqJob, JtEtlJob, JtJob }
import org.apache.spark.sql.SparkSession

object JtApplication extends JtConfigParser with LazyLogging {

  private var conf: JtConfiguration     = _
  implicit private var ss: SparkSession = _
  private[application] var job: JtJob   = _

  def init(configurationFilePath: String): Unit = {
    if (conf == null) {
      logger.info("Reading and parsing the configurations...")
      conf = parse(configurationFilePath).toTry.get
      logger.info("Configuration parsed! Creating the SparkSession...")
      ss = JtSessionFactory.apply().getOrCreate(conf.session).toTry.get
      logger.info("SparkSession created!")
      logger.info("Initializing the job")
      job = createJob()
    }
  }

  private def createJob() = {
    conf.jobType match {
      case DQ     =>
        logger.info("Configuring a data quality job...")
        val jobConf = conf.jobConfiguration.asInstanceOf[JtDQJobConfiguration]
        new JtDqJob(
          jobConf,
          instantiateDqStrategy(jobConf.dqStrategyClass),
          getMaybeIntegrateTableFunction(jobConf.dataDestination.detail),
          getMaybeIntegrateTableFunction(jobConf.badDataDestination.detail),
          jobConf.reportDestination.flatMap(x => getMaybeIntegrateTableFunction(x.detail))
        )
      case ETL    =>
        logger.info("Configuring an ETL job...")
        val jobConf = conf.jobConfiguration.asInstanceOf[JtEtlJobConfiguration]
        new JtEtlJob(
          jobConf,
          instantiateEtlStrategy(jobConf.etlStrategyClass),
          getMaybeIntegrateTableFunction(jobConf.dataDestination.detail)
        )
      case ETL_DQ =>
        logger.info("Configuring an ETL and data quality job...")
        val jobConf = conf.jobConfiguration.asInstanceOf[JtEtlDQJobConfiguration]
        new JtEtlDqJob(
          jobConf,
          instantiateEtlStrategy(jobConf.etlStrategyClass),
          instantiateDqStrategy(jobConf.dqStrategyClass),
          getMaybeIntegrateTableFunction(jobConf.dataDestination.detail),
          getMaybeIntegrateTableFunction(jobConf.badDataDestination.detail),
          jobConf.reportDestination.flatMap(x => getMaybeIntegrateTableFunction(x.detail))
        )
    }
  }

  def getSparkSession: Option[SparkSession] = { Option(ss) }

  def close(): Unit = {
    conf = null
    ss.close()
  }

  def run(): Unit = {
    if (ss == null) { throw new UnsupportedOperationException("Application not initialized! Impossible to run it.") }

    try {
      logger.info("Running the job...")
      job.run()
    } finally {
      logger.info("Job execution terminated!")
      ss.close()
    }
  }

  private def instantiateDqStrategy(dqStrategyClass: String) = {
    Class
      .forName(dqStrategyClass)
      .asSubclass(classOf[JtDqStrategy])
      .getConstructor()
      .newInstance()
  }

  private def instantiateEtlStrategy(etlStrategyClass: String) = {
    Class
      .forName(etlStrategyClass)
      .asSubclass(classOf[JtEtlStrategy])
      .getConstructor()
      .newInstance()
  }

  private def getMaybeIntegrateTableFunction(
    destinationDetail: DestinationDetail
  )(implicit ss: SparkSession): Option[IntegrationTableFunction] = {
    destinationDetail match {
      case DestinationDetail.SparkTable(_, _, _, mode, Some(ifClass), _, _, _, _, _) =>
        mode match {
          case Custom =>
            Some(
              Class
                .forName(ifClass)
                .asSubclass(classOf[IntegrationTableFunction])
                .getConstructor(classOf[SparkSession])
                .newInstance(ss)
            )
          case _      => None
        }

      case _ => None
    }
  }
}
