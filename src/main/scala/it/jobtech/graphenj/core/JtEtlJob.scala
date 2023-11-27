package it.jobtech.graphenj.core

import com.typesafe.scalalogging.LazyLogging
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtEtlJobConfiguration
import it.jobtech.graphenj.core.strategy.JtEtlStrategy
import it.jobtech.graphenj.core.utils.{ CoreReadingUtils, CoreSavingUtils }
import it.jobtech.graphenj.reader.JtSourceReader
import it.jobtech.graphenj.writer.JtDestinationWriter
import org.apache.spark.sql.SparkSession

class JtEtlJob(
  conf: JtEtlJobConfiguration,
  strategy: JtEtlStrategy,
  maybeIntegrationFunction: Option[IntegrationTableFunction]
)(implicit val ss: SparkSession)
    extends JtJob
    with LazyLogging {

  private lazy val jtSourceReader          = new JtSourceReader()
  private lazy val jtDestinationWriter     = new JtDestinationWriter(maybeIntegrationFunction)
  private lazy val bookmarkRepositoriesMap = CoreSavingUtils.getBookmarkRepositoriesMap(conf.sources)

  /** This method performs the following steps
    *   - reading the data source
    *   - transformation strategy application
    *   - bookmarks saving
    *   - transformed data saving
    *   - initial state restoring if an error occurred during the data saving (in this case bookmarks saved previously
    *     are deleted)
    */
  override def run(): Unit = {
    val sourceDfs = CoreReadingUtils.readAllSources(conf.sources, jtSourceReader)

    strategy.transform(sourceDfs) match {
      case Left(exception)      => throw exception
      case Right(transformedDf) =>
        (if (bookmarkRepositoriesMap.nonEmpty) {
           CoreSavingUtils.saveResultAndBookmarks(
             sourceDfs,
             transformedDf,
             jtDestinationWriter,
             conf.dataDestination,
             conf.sources,
             bookmarkRepositoriesMap
           )
         } else {
           jtDestinationWriter.write(transformedDf, conf.dataDestination)
         }) match {
          case Left(exception) => throw exception
          case Right(_)        => logger.info("Data written with successful!")
        }
    }
  }

}
