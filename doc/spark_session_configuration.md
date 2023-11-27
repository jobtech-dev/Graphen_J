# SparkSession configurations #

Graphen_J is a framework based on ApacheSpark, therefore it expects the initialization of the SparkSession.
In this section it will be explained how to configure the SparkSession via configuration file.

To define the SparkSession you need to provide the following configurations:

- `sessionType`: `local` if you want to run the framework locally, `distributed` if you want to run the framework in
  distributed mode
- `applicationName`: the name of the application
- `conf`: a map of SparkSession configurations
- `icebergSupport`: `true` if you are going to use Apache Iceberg, `false` otherwise
- `icebergCatalogs`: a sequence of configurations to specify the icebergCatalogs. You can choose to use the
  HadoopApacheIcebergCatalog or GlueApacheIcebergCatalog
    - HadoopApacheIcebergCatalog
        - `catalogName`: name of the Hadoop catalog
        - `warehouse`: path of the warehouse
        - `endpoint`: endpoint where is the catalog. This is an optional field
        - `catalogType`: you have to write in this field the `hadoop` value
    - GlueApacheIcebergCatalog
        - `catalogName`: name of the Glue catalog
        - `warehouse`: path of the warehouse
        - `catalogType`: you have to write in this field the `glue` value

#### Example

```json
{
  "session": {
    "sessionType": "${SESSION_TYPE}",
    "applicationName": "test-application",
    "conf": {
      "option1": "value1",
      "option2": "value2"
    },
    "icebergSupport": true,
    "icebergCatalogs": [
      {
        "hadoopCatalogName": "source_dq_catalog",
        "warehouse": "dq_warehouse_path"
      },
      {
        "glueCatalogName": "destination_dq_catalog",
        "warehouse": "gold_warehouse_path"
      }
    ],
    "checkpointDir": "checkpoint_dir"
  }
}
```