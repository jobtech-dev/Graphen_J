package it.jobtech.graphenj.core

import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.{ VerificationResult, VerificationSuite }
import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.JtDestination
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtDQJobConfiguration
import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail.{ Database, SparkTable }
import it.jobtech.graphenj.configuration.model.bookmark.{ DB, FS, JtBookmarkStorageDetail, OS }
import it.jobtech.graphenj.core.strategy.JtDqStrategy
import it.jobtech.graphenj.core.utils.CoreSavingUtils
import it.jobtech.graphenj.reader.JtSourceReader
import it.jobtech.graphenj.repositories.{ JtBookmarkRepository, JtBookmarkRepositoryFsOs, JtBookmarkRepositoryJDBC }
import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.utils.JtError.BookmarkConfigurationError
import it.jobtech.graphenj.writer.JtDestinationWriter
import org.apache.spark.sql.{ DataFrame, SparkSession }

class JtDqJob(
  conf: JtDQJobConfiguration,
  dqStrategy: JtDqStrategy,
  maybeDataDestinationIntegrationFunction: Option[IntegrationTableFunction],
  maybeBadDataDestinationIntegrationFunction: Option[IntegrationTableFunction],
  maybeReportDestinationIntegrationFunction: Option[IntegrationTableFunction]
)(implicit val ss: SparkSession)
    extends JtJob
    with LazyLogging {

  private lazy val jtDqSourceReader: JtSourceReader = new JtSourceReader()
  private lazy val jtDataDestinationWriter          = new JtDestinationWriter(maybeDataDestinationIntegrationFunction)
  private lazy val jtBadDataDestinationWriter       = new JtDestinationWriter(maybeBadDataDestinationIntegrationFunction)
  private lazy val jtReportDestinationWriter        = new JtDestinationWriter(maybeReportDestinationIntegrationFunction)

  private lazy val maybeBookmarkRepository: Option[JtBookmarkRepository[JtBookmarkStorageDetail]] =
    conf.source.bookmarkConf.map(storage =>
      storage.bookmarkStorage.storageType match {
        case FS | OS => new JtBookmarkRepositoryFsOs(storage.bookmarkStorage.detail.asInstanceOf[SparkTable])
        case DB      => new JtBookmarkRepositoryJDBC(storage.bookmarkStorage.detail.asInstanceOf[Database])
        case t       =>
          throw BookmarkConfigurationError(
            new IllegalArgumentException(s"Storage type $t not supported for bookmarks!")
          )
      }
    )

  /** This method performs the followings steps:
    *   - reading the data source
    *   - data quality checking
    *   - writing data to the appropriate destination (goodDataDestination, badDataDestination)
    *   - report saving (if specified in the configuration file)
    */
  override def run(): Unit = {
    val sourceDf = jtDqSourceReader.read(conf.source.detail, conf.source.bookmarkConf) match {
      case Left(exception) =>
        logger.error("An error occurred during the reading phase!")
        throw exception
      case Right(sourceDf) => sourceDf
    }

    if (!sourceDf.isEmpty) {
      val verificationResult =
        VerificationSuite()
          .onData(sourceDf)
          .useSparkSession(ss)
          .addChecks(dqStrategy.checkQuality)
          .run()

      checkVerificationResultAndSaveData(sourceDf, verificationResult) match {
        case Left(exception) => throw exception
        case Right(_)        =>
          CoreSavingUtils.saveDqReport(conf.reportDestination, verificationResult, jtReportDestinationWriter) match {
            case Some(value) =>
              value match {
                case Left(e)  => throw e
                case Right(_) =>
                  logger.info("The data was successfully checked and saved as well as the quality check report!")
              }
            case None        => logger.info("The data has been checked and saved successfully!")
          }
      }
    } else {
      logger.info("No data to check.")
    }
  }

  private def checkVerificationResultAndSaveData(
    sourceDf: DataFrame,
    verificationResult: VerificationResult
  ): Either[JtError, Unit] = {
    def saveBadData: Either[JtError, Unit] = {
      maybeBookmarkRepository
        .map(repo => saveDataAndBookmarks(sourceDf, conf.badDataDestination, jtBadDataDestinationWriter, repo))
        .getOrElse(jtBadDataDestinationWriter.write(sourceDf, conf.badDataDestination))
    }

    verificationResult.status match {
      case CheckStatus.Success =>
        logger.info("The data passed the data quality check. Saving them into the destination.")
        maybeBookmarkRepository
          .map(repository => saveDataAndBookmarks(sourceDf, conf.dataDestination, jtDataDestinationWriter, repository))
          .getOrElse(jtDataDestinationWriter.write(sourceDf, conf.dataDestination))

      case CheckStatus.Warning =>
        logger.info("Data quality checking not passed: some warnings are found!")
        saveBadData

      case CheckStatus.Error =>
        logger.info("Data quality checking not passed: some errors are found!")
        saveBadData
    }
  }

  private def saveDataAndBookmarks(
    sourceDf: DataFrame,
    destination: JtDestination,
    writer: JtDestinationWriter,
    bookmarkRepository: JtBookmarkRepository[JtBookmarkStorageDetail]
  ): Either[JtError, Unit] = {
    CoreSavingUtils
      .saveBookmark(bookmarkRepository, sourceDf, conf.source.bookmarkConf.get.bookmarkDetail)
      .flatMap(_ => {
        writer
          .write(sourceDf, destination)
          .left
          .map(error => {
            logger.error("An error occurred during the data writing: restoring the initial state!")
            bookmarkRepository
              .deleteLastBookmark(conf.source.bookmarkConf.get.bookmarkDetail.bookmarkEntry)
              .left
              .toOption
              .getOrElse(error)
          })
      })
  }
}
