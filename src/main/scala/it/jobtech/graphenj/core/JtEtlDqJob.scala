package it.jobtech.graphenj.core

import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.{ VerificationResult, VerificationSuite }
import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtEtlDQJobConfiguration
import it.jobtech.graphenj.core.strategy.{ JtDqStrategy, JtEtlStrategy }
import it.jobtech.graphenj.core.utils.{ CoreReadingUtils, CoreSavingUtils }
import it.jobtech.graphenj.reader.JtSourceReader
import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.writer.JtDestinationWriter
import org.apache.spark.sql.{ DataFrame, SparkSession }

class JtEtlDqJob(
  conf: JtEtlDQJobConfiguration,
  etlStrategy: JtEtlStrategy,
  dqStrategy: JtDqStrategy,
  maybeDataDestinationIntegrationFunction: Option[IntegrationTableFunction],
  maybeBadDataDestinationIntegrationFunction: Option[IntegrationTableFunction],
  maybeReportDestinationIntegrationFunction: Option[IntegrationTableFunction]
)(implicit val ss: SparkSession)
    extends JtJob
    with LazyLogging {

  private lazy val jtSourceReader             = new JtSourceReader()
  private lazy val jtDataDestinationWriter    = new JtDestinationWriter(maybeDataDestinationIntegrationFunction)
  private lazy val jtBadDataDestinationWriter = new JtDestinationWriter(maybeBadDataDestinationIntegrationFunction)
  private lazy val jtReportDestinationWriter  = new JtDestinationWriter(maybeReportDestinationIntegrationFunction)
  private lazy val bookmarkRepositoriesMap    = CoreSavingUtils.getBookmarkRepositoriesMap(conf.sources)

  override def run(): Unit = {
    val sourceDfs = CoreReadingUtils.readAllSources(conf.sources, jtSourceReader)

    etlStrategy.transform(sourceDfs) match {
      case Left(exception) => throw exception

      case Right(transformedDf) =>
        val verificationResult = VerificationSuite()
          .onData(transformedDf)
          .useSparkSession(ss)
          .addChecks(dqStrategy.checkQuality)
          .run()

        (if (bookmarkRepositoriesMap.nonEmpty) {
           checkVerificationResultAndSaveDataAndBookmarks(sourceDfs, transformedDf, verificationResult)
         } else {
           jtDataDestinationWriter.write(transformedDf, getDestination(verificationResult))
         }) match {
          case Left(exception) => throw exception
          case Right(_)        =>
            CoreSavingUtils.saveDqReport(conf.reportDestination, verificationResult, jtReportDestinationWriter) match {
              case Some(value) =>
                value match {
                  case Left(e)  => throw e
                  case Right(_) =>
                    logger.info(
                      "The data was successfully transformed, checked and saved as well as the quality check report!"
                    )
                }
              case None        => logger.info("The data has been transformed, checked and saved successfully!")
            }
        }
    }
  }

  private def getDestination(verificationResult: VerificationResult) = {
    verificationResult.status match {
      case CheckStatus.Success =>
        logger.info("The data passed the data quality check. Saving them into the destination.")
        conf.dataDestination
      case CheckStatus.Warning =>
        logger.info("Data quality checking not passed: some warnings are found!")
        conf.badDataDestination
      case CheckStatus.Error   =>
        logger.info("Data quality checking not passed: some errors are found!")
        conf.badDataDestination
    }
  }

  private def checkVerificationResultAndSaveDataAndBookmarks(
    sourceDfs: Map[String, DataFrame],
    transformedDf: DataFrame,
    verificationResult: VerificationResult
  ): Either[JtError, Unit] = {
    verificationResult.status match {
      case CheckStatus.Success =>
        logger.info("The data passed the data quality check. Saving them into the destination.")
        CoreSavingUtils.saveResultAndBookmarks(
          sourceDfs,
          transformedDf,
          jtDataDestinationWriter,
          conf.dataDestination,
          conf.sources,
          bookmarkRepositoriesMap
        )
      case CheckStatus.Warning =>
        logger.info("Data quality checking not passed: some warnings are found!")
        CoreSavingUtils.saveResultAndBookmarks(
          sourceDfs,
          transformedDf,
          jtBadDataDestinationWriter,
          conf.badDataDestination,
          conf.sources,
          bookmarkRepositoriesMap
        )
      case CheckStatus.Error   =>
        logger.info("Data quality checking not passed: some errors are found!")
        CoreSavingUtils.saveResultAndBookmarks(
          sourceDfs,
          transformedDf,
          jtBadDataDestinationWriter,
          conf.badDataDestination,
          conf.sources,
          bookmarkRepositoriesMap
        )
    }
  }
}
