![](doc/assets/Graphen_J.svg)

Graphen_J is a framework written in the Scala language, built on top of Apache Spark
and [Deequ](https://github.com/awslabs/deequ) to perform EL, ETL and Data Quality processes for large datasets.

The goal behind the creation of this framework is to simplify and speed up the development of these processes. The
developer will be able to take advantage of various features by simply writing a configuration file and focus only on
the development of core processes.

## Requirements and installation

Graphen_J depends on:

- Java 8
- Spark
- Apache Iceberg
- Scala 2.12

## Features

Graphen_J offers the following features:

- Creating different types of jobs to perform: ETL, DQ, ETL_DQ
- Definition through the configurations of multiple data sources from which to read data
- Definition through the configurations of the data destination
- Bookmarking functionality to define through the configurations
- Creating a data transformation strategy
- Creating a data quality strategy
- Defining a custom function to integrate data into tables created using providers such as Apache Iceberg

## Documentation

To see the detailed documentation of the framework refer to [Documentation](doc/README.md)