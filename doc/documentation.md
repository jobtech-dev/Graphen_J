# Documentation #

The strength of this framework is the definition of many operations through a simple configuration file.

The framework supports both Yaml and Json configuration files and is able to parse the environment variables specified
within them.
To use an environment variable you can use the following syntax `${ENV_VARIABLE}`.

## Structure of the configurations

The configurations to be specified are the following:

- `session`: [configurations](spark_session_configuration.md) to specify the creation of the SparkSession
- `sources`: [configurations](source_configurations.md) to specify the data sources to read
- `dataDestination`: [configurations](destination_configuration.md) to specify the destination of the data
- `etlStrategyClass`: Graphen_J offers implementation of strategy `it.jobtech.jtetl.core.strategy.JtIdentityStrategy`. This
  strategy returns the data as it was read, the only requirement is that there must be only one source. If multiple
  sources are read, an application error will be returned.

## [How to define a custom strategy](how_to_define_an_etl_strategy.md) 
## [How to define a custom integration table function](how_to_define_a_custom_integration_table_function.md) 