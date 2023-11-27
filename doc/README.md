# Documentation #

Graphen_J is a framework written in the Scala language, built on top of Apache Spark
and [Deequ](https://github.com/awslabs/deequ) to perform EL, ETL and Data Quality processes for large datasets.

The strength of this framework is the definition of many operations through a simple configuration file.

The framework supports both Yaml and Json configuration files and is able to parse the environment variables specified
within them.

## Structure of the configurations

The configurations to be specified are the following:

- `jobType`: configurations to specify the type of the job you want to perform. Available values are ETL, DQ, ETL_DQ
- `session`: [configurations](spark_session_configuration.md) to specify the creation of the SparkSession
- `jobConfiguration`: configurations for the job you want to perform [ETL](etl_job_configuration.md)
  , [DQ](dq_job_configuration.md), [ETL_DQ](etl_dq_job_configuration.md)

## [How to define a custom strategy](how_to_define_an_etl_strategy.md)

## [How to define a data quality strategy](how_to_define_a_dq_strategy.md)

## [How to define a custom integration table function](how_to_define_a_custom_integration_table_function.md)
