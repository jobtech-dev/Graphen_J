# Data destination configurations #

Graphen_J allows the writing into different destination compatibly with Apache Spark.
In this section it will be explained how to configure the data destination and its writing.

## How To define a data destination

You can define a data destination using two types of configuration: Format or SparkTable depending on whether you want
to write a format (e.g. jdbc, files, ...) or a table (e.g. Apache Iceberg Table, ...).

### Destination Format Type

To define a Destination of the Format type you have to specify the following configurations:

- `id`: this is the id of the source
- `destinationDetail`: this is an object as follows:
    - `format`: the format you want to write (e.g. csv, parquet, jdbc, ...)
    - `options`: the Apache Spark writing options (e.g. path, password, ...)
    - `mode`: the ApacheSpark SaveMode (Append, Overwrite, ErrorIfExists, Ignore)
    - `partitionKeys`: a sequence of partition keys. It can be an empty list

#### Example

```json
{
  "id": "dataDestination",
  "detail": {
    "format": "parquet",
    "options": {
      "path": "data_destination_path"
    },
    "mode": "append",
    "partitionKeys": [
      "field1",
      "field2"
    ]
  }
}
```

### Destination SparkTable Type

To define a Destination of the SparkTable type you have to specify the following configurations:

- `id`: this is the id of the source
- `detail`: this is an object as follows:
    - `catalogName`: the name of the catalog
    - `dbName`: the name of the database
    - `tableName`: the name of the table
    - `mode`: this configuration indicates the way we want to write the data. Supported modes are as follows:
        - `ErrorIfExists`: trigger an error if the table you are trying to write already exists;
        - `Overwrite`: Create or overwrite a table if it already exists;
        - `Append`: Create a table if it doesn't exist or add data if it already exists;
        - `MergeIntoById`: create a table if it does not exist, add data if it already exists, if in the existing table
          there is a data with the same id as one you are trying to write, it will be updated;
        - `Custom`: custom mode defined by the user (it will be necessary, as specified below, to indicate the
          customized class implemented);
    - `integrationTableFunctionClass`: this field is required only if the field _mode_ assumes the value `Custom` and
      have to specify the class that will take care of integrating the data into the
      table. [Here](how_to_define_a_custom_integration_table_function.md) it is explained how to implement a custom
      IntegrationTableFunction.
    - `idFields`: this field is mandatory in case the _mode_ field assumes the value `MergeIntoById`. You must indicate
      a list of fields representing the id of each row
    - `provider`: this is an optional parameter. You have to specify the provider of the table. For now available
      provider is `iceberg`
    - `tableProperties`: this is an optional configuration, and it is a map of table properties
    - `options`: this is an optional configuration, and it's a map of Apache Spark writing options
    - `partitionKeys`: a sequence of partition keys. You may not specify this configuration if you do not want to
      partition your data

#### Example

```json
{
  "id": "destination",
  "detail": {
    "catalogName": "${DESTINATION_CATALOG}",
    "dbName": "${DB}",
    "tableName": "${TABLE}",
    "mode": "Custom",
    "integrationTableFunctionClass": "it.jobtech.jtetl.application.PeopleIntegrationTableFunction",
    "idFields": [],
    "provider": "iceberg",
    "tableProperties": {
      "write.format.default": "parquet",
      "format-version": "2",
      "write.parquet.compression-codec": "snappy"
    },
    "options": {
      "merge-schema": "true"
    }
  }
}
```