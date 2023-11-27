# ETL_DQ (ETL and Data Quality) job configurations #

To configure an ETL and DQ (Data Quality) job you need to provide the following configurations:

- `sources`: [configurations](source_configurations.md) to specify a list of data source to read
- `dataDestination`: [configurations](destination_configuration.md) to specify the destination of the data if they pass
  the data quality check
- `badDataDestination`: [configurations](destination_configuration.md) to specify the destination of the data if they
  don't pass the data quality checks
- `reportDestination`: [configurations](destination_configuration.md) to specify the destination of the report. It's an
  optional configuration, then if you don't want to save the reports you can omit it
- `etlStrategyClass`: The class with which perform the data transformation. Graphen_J offers implementation of
  strategy `it.jobtech.jtetl.core.strategy.JtIdentityStrategy`. This strategy returns the data as it was read, the only
  requirement is that there must be only one source. If multiple sources are read, an application error will be
  returned.
- `dqStrategyClass`: class that performs the data quality checks

#### Example

```json
{
  "jobType": "ETL_DQ",
  "session": {
    "sessionType": "local",
    "applicationName": "test-application",
    "icebergSupport": true,
    "icebergCatalogs": [
      {
        "hadoopCatalogName": "source_catalog",
        "warehouse": "source_warehouse_path"
      },
      {
        "glueCatalogName": "destination_catalog",
        "warehouse": "dest_warehouse_path"
      }
    ]
  },
  "jobConfiguration": {
    "sources": [
      {
        "id": "source1",
        "detail": {
          "format": "jdbc",
          "options": {
            "url": "jdbc:mysql://localhost:3306",
            "driver": "com.mysql.jdbc.Driver",
            "db": "db",
            "user": "user",
            "password": "password"
          }
        },
        "bookmarkConf": {
          "bookmarkDetail": {
            "bookmarkEntry": "source1",
            "bookmarkField": "updatedAt",
            "bookmarkFieldType": "Long"
          },
          "bookmarkStorage": {
            "id": "bookmarks",
            "storageType": "OS",
            "detail": {
              "catalogName": "bookmark_catalog",
              "dbName": "db",
              "tableName": "bookmarks",
              "provider": "iceberg",
              "options": {
                "write.format.default": "parquet",
                "format-version": "2",
                "write.parquet.compression-codec": "lz4"
              }
            }
          }
        }
      },
      {
        "id": "source2",
        "detail": {
          "catalogName": "source_catalog",
          "dbName": "source_db",
          "tableName": "source_table",
          "provider": "iceberg"
        },
        "bookmarkConf": {
          "bookmarkDetail": {
            "bookmarkEntry": "source2",
            "bookmarkField": "extracted",
            "bookmarkFieldType": "Date",
            "bookmarkFieldFormat": "YYYY/mm/dd"
          },
          "bookmarkStorage": {
            "id": "bookmarks",
            "storageType": "OS",
            "detail": {
              "catalogName": "bookmark_catalog",
              "dbName": "db",
              "tableName": "bookmarks",
              "provider": "iceberg",
              "options": {
                "write.format.default": "parquet",
                "format-version": "2",
                "write.parquet.compression-codec": "lz4"
              }
            }
          }
        }
      }
    ],
    "dataDestination": {
      "id": "destination",
      "detail": {
        "format": "parquet",
        "options": {
          "path": "data_destination_path"
        },
        "mode": "Append",
        "partitionKeys": [
          "field1",
          "field2"
        ]
      }
    },
    "badDataDestination": {
      "id": "badDestination",
      "detail": {
        "format": "parquet",
        "options": {
          "path": "bad_data_destination_path"
        },
        "mode": "Append",
        "partitionKeys": [
          "field1",
          "field2"
        ]
      }
    },
    "reportDestination": {
      "id": "reportDestination",
      "detail": {
        "format": "parquet",
        "options": {
          "path": "report_destination_path"
        },
        "mode": "Append",
        "partitionKeys": [
          "field1",
          "field2"
        ]
      }
    },
    "etlStrategyClass": "it.jobtech.jtetl.core.strategy.JtIdentityStrategy",
    "dqStrategyClass": "it.jobtech.jtetl.core.strategy.PeopleDqStrategy"
  }
}
```