# ETL job configurations #

To configure an ETL job you need to provide the following configurations:

- `sources`: [configurations](source_configurations.md) to specify a list of data sources to read
- `dataDestination`: [configurations](destination_configuration.md) to specify the destination of the data
- `etlStrategyClass`: The class with which perform the data transformation. Graphen_J offers implementation of
  strategy `it.jobtech.jtetl.core.strategy.JtIdentityStrategy`. This strategy returns the data as it was read, the only
  requirement is that there must be only one source. If multiple sources are read, an application error will be
  returned.

#### Example

```json
{
  "jobType": "ETL",
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
        "catalogName": "destination_catalog",
        "dbName": "destination_db",
        "tableName": "destination_table",
        "mode": "Custom",
        "integrationTableFunctionClass": "it.jobtech.jtetl.application.PeopleIntegrationTableFunction",
        "provider": "iceberg",
        "options": {
          "write.format.default": "parquet",
          "format-version": "2",
          "write.parquet.compression-codec": "lz4"
        }
      }
    },
    "etlStrategyClass": "it.jobtech.jtetl.core.strategy.JtIdentityStrategy"
  }
}
```