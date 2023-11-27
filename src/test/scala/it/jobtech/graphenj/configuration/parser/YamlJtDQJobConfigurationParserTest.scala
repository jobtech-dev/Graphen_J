package it.jobtech.graphenj.configuration.parser

import io.circe.generic.auto._
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtDQJobConfiguration
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkLong,
  JtBookmarkStorage,
  JtBookmarkStorageDetail,
  OS
}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class YamlJtDQJobConfigurationParserTest extends AnyFunSuite with Matchers with YamlConfigParser {

  test("parse complete configurations") {
    System.setProperty("SESSION_TYPE", "local")
    val yml =
      """source:
        |  id: source1
        |  detail:
        |    format: jdbc
        |    options:
        |      url: jdbc:mysql://localhost:3306
        |      driver: com.mysql.jdbc.Driver
        |      db: db
        |      user: user
        |      password: password
        |  bookmarkConf:
        |    bookmarkDetail:
        |      bookmarkEntry: source1
        |      bookmarkField: updatedAt
        |      bookmarkFieldType: Long
        |    bookmarkStorage:
        |      id: bookmarks
        |      storageType: OS
        |      detail:
        |        catalogName: bookmark_catalog
        |        dbName: db
        |        tableName: bookmarks
        |        provider: iceberg
        |        tableProperties:
        |          write.format.default: parquet
        |          format-version: '2'
        |          write.parquet.compression-codec: lz4
        |dataDestination:
        |  id: destination
        |  detail:
        |    format: parquet
        |    options:
        |      path: data_destination_path
        |    mode: Append
        |    partitionKeys:
        |    - field1
        |    - field2
        |badDataDestination:
        |  id: badDestination
        |  detail:
        |    format: parquet
        |    options:
        |      path: bad_data_destination_path
        |    mode: Append
        |    partitionKeys:
        |    - field1
        |    - field2
        |reportDestination:
        |  id: reportDestination
        |  detail:
        |    format: parquet
        |    options:
        |      path: report_destination_path
        |    mode: Append
        |    partitionKeys:
        |    - field1
        |    - field2
        |dqStrategyClass: my_strategy""".stripMargin

    val expectedRes = JtDQJobConfiguration(
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
      "my_strategy"
    )
    val res         = parseYamlConfig[JtDQJobConfiguration](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }
}
