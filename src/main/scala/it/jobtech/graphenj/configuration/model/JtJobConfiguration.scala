package it.jobtech.graphenj.configuration.model

trait JtJobConfiguration extends Product with Serializable

object JtJobConfiguration {

  /** Configuration for an ETL job
    * @param sources:
    *   configuration for the sources to read
    * @param dataDestination:
    *   configuration for the data destination
    * @param etlStrategyClass:
    *   class that implement the strategy
    */
  case class JtEtlJobConfiguration(
    sources: Seq[JtSource],
    dataDestination: JtDestination,
    etlStrategyClass: String
  ) extends JtJobConfiguration

  /** Configuration for a DQ job
    * @param source:
    *   configuration for the source to read
    * @param dataDestination:
    *   configuration for the data destination
    * @param badDataDestination:
    *   configuration for the bad data destination
    * @param reportDestination:
    *   configuration for the data quality report destination
    * @param dqStrategyClass:
    *   class that implement the quality checking
    */
  case class JtDQJobConfiguration(
    source: JtSource,
    dataDestination: JtDestination,
    badDataDestination: JtDestination,
    reportDestination: Option[JtDestination],
    dqStrategyClass: String
  ) extends JtJobConfiguration

  /** Configuration for an ETL_DQjob
    * @param sources:
    *   configuration for the sources to read
    * @param dataDestination:
    *   configuration for the data destination
    * @param badDataDestination:
    *   configuration for the bad data destination
    * @param reportDestination:
    *   configuration for the data quality report destination
    * @param etlStrategyClass:
    *   class that implement the strategy
    * @param dqStrategyClass:
    *   class that implement the quality checking
    */
  case class JtEtlDQJobConfiguration(
    sources: Seq[JtSource],
    dataDestination: JtDestination,
    badDataDestination: JtDestination,
    reportDestination: Option[JtDestination],
    etlStrategyClass: String,
    dqStrategyClass: String
  ) extends JtJobConfiguration
}
