package it.jobtech.graphenj.configuration.parser

import io.circe.generic.auto._
import it.jobtech.graphenj.configuration.model.ApacheIcebergCatalog.{
  GlueApacheIcebergCatalog,
  HadoopApacheIcebergCatalog
}
import it.jobtech.graphenj.configuration.model.SessionType.Local
import it.jobtech.graphenj.configuration.model.bookmark._
import it.jobtech.graphenj.configuration.model._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class YamlGeneralConfigurationParsing extends AnyFunSuite with Matchers with YamlConfigParser {

  test("parse session configuration with iceberg") {
    val yml         =
      """sessionType: local
        |applicationName: test-application
        |icebergSupport: true
        |icebergCatalogs:
        |- catalogName: source_catalog
        |  warehouse: source_warehouse_path
        |  catalogType: hadoop
        |- catalogName: destination_catalog
        |  warehouse: dest_warehouse_path
        |  catalogType: glue""".stripMargin
    val expectedRes = JtSession(
      Local,
      "test-application",
      None,
      Some(true),
      Some(
        Seq(
          HadoopApacheIcebergCatalog("source_catalog", "source_warehouse_path", None),
          GlueApacheIcebergCatalog("destination_catalog", "dest_warehouse_path")
        )
      )
    )
    val res         = parseYamlConfig[JtSession](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse session configuration without iceberg") {
    val yml         = """sessionType: local
                |applicationName: test-application""".stripMargin
    val expectedRes = JtSession(
      Local,
      "test-application",
      None,
      None,
      None
    )
    val res         = parseYamlConfig[JtSession](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse source format configuration") {
    val yml         =
      """id: source1
        |detail:
        |  format: jdbc
        |  options:
        |    url: jdbc:mysql://localhost:3306
        |    driver: com.mysql.jdbc.Driver
        |    db: db
        |    user: user
        |    password: password
        |bookmarkConf:
        |  bookmarkDetail:
        |    bookmarkEntry: source1
        |    bookmarkField: updatedAt
        |    bookmarkFieldType: Long
        |  bookmarkStorage:
        |    id: bookmarks
        |    storageType: OS
        |    detail:
        |      catalogName: bookmark_catalog
        |      dbName: db
        |      tableName: bookmarks
        |      provider: iceberg
        |      tableProperties:
        |        write.format.default: parquet
        |        format-version: '2'
        |        write.parquet.compression-codec: lz4""".stripMargin
    val expectedRes = JtSource(
      "source1",
      SourceDetail.Format(
        "jdbc",
        None,
        Map(
          "url"      -> "jdbc:mysql://localhost:3306",
          "driver"   -> "com.mysql.jdbc.Driver",
          "db"       -> "db",
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
    )
    val res         = parseYamlConfig[JtSource](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse source spark table configuration") {
    val yml =
      """
        |id: source2
        |detail:
        |  catalogName: source_catalog
        |  dbName: source_db
        |  tableName: source_table
        |  provider: iceberg
        |bookmarkConf:
        |  bookmarkDetail:
        |    bookmarkEntry: source2
        |    bookmarkField: extracted
        |    bookmarkFieldType: Date
        |    bookmarkFieldFormat: YYYY/mm/dd
        |  bookmarkStorage:
        |    id: bookmarks
        |    storageType: OS
        |    detail:
        |      catalogName: bookmark_catalog
        |      dbName: db
        |      tableName: bookmarks
        |      provider: iceberg
        |      tableProperties:
        |        write.format.default: parquet
        |        format-version: '2'
        |        write.parquet.compression-codec: lz4""".stripMargin

    val expectedRes = JtSource(
      "source2",
      SourceDetail.SparkTable(
        "source_catalog",
        "source_db",
        "source_table",
        Some(Iceberg)
      ),
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
    val res         = parseYamlConfig[JtSource](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse destination format configuration") {
    val yml         =
      """id: dataDestination
        |detail:
        |  format: parquet
        |  options:
        |    path: data_destination_path
        |  mode: append
        |  partitionKeys:
        |  - field1
        |  - field2""".stripMargin
    val expectedRes = JtDestination(
      "dataDestination",
      DestinationDetail.Format(
        "parquet",
        Map("path" -> "data_destination_path"),
        SaveMode.Append,
        Seq("field1", "field2")
      )
    )
    val res         = parseYamlConfig[JtDestination](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse destination spark table configuration") {
    val yml         =
      """id: destination
        |detail:
        |  catalogName: destination_catalog
        |  dbName: destination_db
        |  tableName: destination_table
        |  mode: append
        |  provider: iceberg
        |  tableProperties:
        |    write.format.default: parquet
        |    format-version: '2'
        |    write.parquet.compression-codec: lz4""".stripMargin
    val expectedRes = JtDestination(
      "destination",
      DestinationDetail.SparkTable(
        "destination_catalog",
        "destination_db",
        "destination_table",
        Append,
        None,
        Seq.empty,
        Some(Iceberg),
        Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "lz4"),
        Map.empty
      )
    )
    val res         = parseYamlConfig[JtDestination](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse destination spark table configuration with MergeIntoTable mode") {
    val yml         =
      """id: destination
        |detail:
        |  catalogName: destination_catalog
        |  dbName: destination_db
        |  tableName: destination_table
        |  mode: MergeIntoById
        |  idFields:
        |  - field1
        |  - field2
        |  provider: iceberg
        |  tableProperties:
        |    write.format.default: parquet
        |    format-version: '2'
        |    write.parquet.compression-codec: lz4""".stripMargin
    val expectedRes = JtDestination(
      "destination",
      DestinationDetail.SparkTable(
        "destination_catalog",
        "destination_db",
        "destination_table",
        MergeIntoById,
        None,
        Seq("field1", "field2"),
        Some(Iceberg),
        Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "lz4")
      )
    )
    val res         = parseYamlConfig[JtDestination](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse bookmark storage configuration") {
    val yml         =
      """id: bookmarks
        |storageType: OS
        |detail:
        |  catalogName: bookmark_catalog
        |  dbName: db
        |  tableName: bookmarks
        |  provider: iceberg
        |  tableProperties:
        |    write.format.default: parquet
        |    format-version: '2'
        |    write.parquet.compression-codec: lz4""".stripMargin
    val expectedRes = JtBookmarkStorage(
      "bookmarks",
      OS,
      JtBookmarkStorageDetail.SparkTable(
        "bookmark_catalog",
        "db",
        "bookmarks",
        Some(Iceberg),
        Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "lz4")
      )
    )
    val res         = parseYamlConfig[JtBookmarkStorage](yml)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }

  test("parse bookmark Database storage configuration") {
    val json        =
      """id: bookmarks
        |entry: my_entry
        |storageType: DB
        |detail:
        |  host: jdbc:mysql:my_host
        |  user: user
        |  password: password
        |  dbName: db
        |  tableName: table
        |  dbType: MySQL""".stripMargin
    val expectedRes = JtBookmarkStorage(
      "bookmarks",
      DB,
      JtBookmarkStorageDetail.Database(
        "jdbc:mysql:my_host",
        "user",
        "password",
        "db",
        "table",
        MySQL
      )
    )
    val res         = parseYamlConfig[JtBookmarkStorage](json)

    res.isRight shouldBe true
    res.right.get shouldBe expectedRes
  }
}
