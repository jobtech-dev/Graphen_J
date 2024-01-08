package it.jobtech.graphenj.configuration.parser

import io.circe.generic.auto._
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtEtlDQJobConfiguration
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkDate,
  JtBookmarkLong,
  JtBookmarkStorage,
  JtBookmarkStorageDetail,
  OS
}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonJtEtlDQJobConfigurationParserTest extends AnyFunSuite with Matchers with JsonConfigParser {

  test("parse complete configurations") {
    System.setProperty("SESSION_TYPE", "local")
    val json =
      """{
        |  "sources": [
        |    {
        |      "id": "source1",
        |      "detail": {
        |        "format": "jdbc",
        |        "options": {
        |          "url": "jdbc:mysql://localhost:3306",
        |          "driver": "com.mysql.jdbc.Driver",
        |          "db": "db",
        |          "user": "user",
        |          "password": "password"
        |        }
        |      },
        |      "bookmarkConf": {
        |        "bookmarkDetail": {
        |          "bookmarkEntry": "source1",
        |          "bookmarkField": "updatedAt",
        |          "bookmarkFieldType": "Long"
        |        },
        |        "bookmarkStorage": {
        |          "id": "bookmarks",
        |          "storageType": "OS",
        |          "detail": {
        |            "catalogName": "bookmark_catalog",
        |            "dbName": "db",
        |            "tableName": "bookmarks",
        |            "provider": "iceberg",
        |            "tableProperties": {
        |              "write.format.default": "parquet",
        |              "format-version": "2",
        |              "write.parquet.compression-codec": "lz4"
        |            }
        |          }
        |        }
        |      }
        |    },
        |    {
        |      "id": "source2",
        |      "detail": {
        |        "catalogName": "source_catalog",
        |        "dbName": "source_db",
        |        "tableName": "source_table",
        |        "provider": "iceberg"
        |      },
        |      "bookmarkConf": {
        |        "bookmarkDetail": {
        |          "bookmarkEntry": "source2",
        |          "bookmarkField": "extracted",
        |          "bookmarkFieldType": "Date",
        |          "bookmarkFieldFormat": "YYYY/mm/dd"
        |        },
        |        "bookmarkStorage": {
        |          "id": "bookmarks",
        |          "storageType": "OS",
        |          "detail": {
        |            "catalogName": "bookmark_catalog",
        |            "dbName": "db",
        |            "tableName": "bookmarks",
        |            "provider": "iceberg",
        |            "tableProperties": {
        |              "write.format.default": "parquet",
        |              "format-version": "2",
        |              "write.parquet.compression-codec": "lz4"
        |            }
        |          }
        |        }
        |      }
        |    }
        |  ],
        |  "dataDestination": {
        |    "id": "destination",
        |    "detail": {
        |      "format": "parquet",
        |      "options": {
        |        "path": "data_destination_path"
        |      },
        |      "mode": "Append",
        |      "partitionKeys": [
        |        "field1",
        |        "field2"
        |      ]
        |    }
        |  },
        |  "badDataDestination": {
        |    "id": "badDestination",
        |    "detail": {
        |      "format": "parquet",
        |      "options": {
        |        "path": "bad_data_destination_path"
        |      },
        |      "mode": "Append",
        |      "partitionKeys": [
        |        "field1",
        |        "field2"
        |      ]
        |    }
        |  },
        |  "reportDestination": {
        |    "id": "reportDestination",
        |    "detail": {
        |      "format": "parquet",
        |      "options": {
        |        "path": "report_destination_path"
        |      },
        |      "mode": "Append",
        |      "partitionKeys": [
        |        "field1",
        |        "field2"
        |      ]
        |    }
        |  },
        |  "etlStrategyClass": "it.jobtech.graphenj.core.strategy.JtIdentityStrategy",
        |  "dqStrategyClass": "my_strategy"
        |}""".stripMargin

    val expectedRes = JtEtlDQJobConfiguration(
      List(
        JtSource(
          "source1",
          SourceDetail.Format(
            "jdbc",
            None,
            Map(
              "url"      -> "jdbc:mysql://localhost:3306",
              "db"       -> "db",
              "driver"   -> "com.mysql.jdbc.Driver",
              "user"     -> "user",
              "password" -> "password"
            )
          ),
          Some(
            JtBookmarksConf(
              JtBookmarkDetail("source1", "updatedAt", JtBookmarkLong, None),
              JtBookmarkStorage(
                "bookmarks",
                OS,
                JtBookmarkStorageDetail.SparkTable(
                  "bookmark_catalog",
                  "db",
                  "bookmarks",
                  Some(Iceberg),
                  Map(
                    "write.format.default"            -> "parquet",
                    "format-version"                  -> "2",
                    "write.parquet.compression-codec" -> "lz4"
                  ),
                  Map.empty
                )
              )
            )
          )
        ),
        JtSource(
          "source2",
          SourceDetail.SparkTable("source_catalog", "source_db", "source_table", Some(Iceberg)),
          Some(
            JtBookmarksConf(
              JtBookmarkDetail("source2", "extracted", JtBookmarkDate, Some("YYYY/mm/dd")),
              JtBookmarkStorage(
                "bookmarks",
                OS,
                JtBookmarkStorageDetail.SparkTable(
                  "bookmark_catalog",
                  "db",
                  "bookmarks",
                  Some(Iceberg),
                  Map(
                    "write.format.default"            -> "parquet",
                    "format-version"                  -> "2",
                    "write.parquet.compression-codec" -> "lz4"
                  ),
                  Map.empty
                )
              )
            )
          )
        )
      ),
      JtDestination(
        "destination",
        DestinationDetail
          .Format("parquet", Map("path" -> "data_destination_path"), SaveMode.Append, List("field1", "field2"))
      ),
      JtDestination(
        "badDestination",
        DestinationDetail
          .Format("parquet", Map("path" -> "bad_data_destination_path"), SaveMode.Append, List("field1", "field2"))
      ),
      Some(
        JtDestination(
          "reportDestination",
          DestinationDetail
            .Format("parquet", Map("path" -> "report_destination_path"), SaveMode.Append, List("field1", "field2"))
        )
      ),
      "it.jobtech.graphenj.core.strategy.JtIdentityStrategy",
      "my_strategy"
    )
    val res         = parseJsonConfig[JtEtlDQJobConfiguration](json)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }
}
