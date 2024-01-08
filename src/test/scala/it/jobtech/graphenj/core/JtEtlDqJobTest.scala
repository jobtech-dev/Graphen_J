package it.jobtech.graphenj.core

import com.amazon.deequ.checks.{ CheckLevel, CheckStatus }
import com.amazon.deequ.constraints.ConstraintStatus
import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtEtlDQJobConfiguration
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  FS,
  JtBookmarkDate,
  JtBookmarkStorage,
  JtBookmarkStorageDetail
}
import it.jobtech.graphenj.core.models.DeequReport
import it.jobtech.graphenj.core.strategy.{ PeopleDqEtlDqTestStrategy, PeopleEtlStrategy }
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.JtError.{ ReadSourceError, WriteError }
import it.jobtech.graphenj.utils.models.{ Person, SparkCatalogInfo }
import it.jobtech.graphenj.utils.{ AssertEqualsIgnoringFields, SparkUtils, TablesFsUtils, Utils }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, to_date }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import scala.util.Try

class JtEtlDqJobTest extends AnyFunSuite with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testPath            = "tmp"
  private val db                  = "db"
  private val table               = "people"
  private val badTable            = "bad_people"
  private val reportTable         = "report_people"
  private val bookmarksTable      = "bookmarks"
  private val partitionKeys       = Seq("extracted")
  private val firstSourceCatalog  = SparkCatalogInfo("source1_catalog", "tmp/source1")
  private val secondSourceCatalog = SparkCatalogInfo("source2_catalog", "tmp/source2")
  private val destinationCatalog  = SparkCatalogInfo("destination_catalog", "tmp/destination")
  private val bookmarksCatalog    = SparkCatalogInfo("bookmarks_catalog", "tmp/bookmarks")
  private val catalogs            = Seq(firstSourceCatalog, secondSourceCatalog, destinationCatalog, bookmarksCatalog)

  private val firstSourceTablePath       = s"${firstSourceCatalog.name}.$db.$table"
  private val secondSourceTablePath      = s"${secondSourceCatalog.name}.$db.$table"
  private val destinationTablePath       = s"${destinationCatalog.name}.$db.$table"
  private val badDestinationTablePath    = s"${destinationCatalog.name}.$db.$badTable"
  private val reportDestinationTablePath = s"${destinationCatalog.name}.$db.$reportTable"
  private val bookmarksTablePath         = s"${bookmarksCatalog.name}.$db.$bookmarksTable"

  implicit private var ss: SparkSession = Utils.createSparkSessionWithIceberg(catalogs)

  private val peopleT1 = Seq(
    Person(1, "harry", "potter", 1672918958, "2023-01-05"),
    Person(2, "jack", "sparrow", 1672918958, "2023-01-06"),
    Person(3, "albus", "silente", 1673005358, "2023-01-07"),
    Person(4, "ace", "ventura", 1673005359, "2023-01-08")
  )

  private val peopleT2 = Seq(
    Person(5, "thomas", "shelby", 1672918958, "2023-01-05"),
    Person(6, "walter", "white", 1672918958, "2023-01-06")
  )

  private val icebertTableProperties: Map[String, String] =
    Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")

  private val jtBookmarkStorage: JtBookmarkStorage =
    JtBookmarkStorage(
      "bookmarkStorage",
      FS,
      JtBookmarkStorageDetail.SparkTable(bookmarksCatalog.name, db, "bookmarks", Some(Iceberg), icebertTableProperties)
    )

  private val jtSources: Seq[JtSource] = Seq(
    JtSource(
      "people1",
      SourceDetail.SparkTable(firstSourceCatalog.name, db, table, Some(Iceberg)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail("source1-people", "extracted", JtBookmarkDate, Some("yyyy-MM-dd")),
          jtBookmarkStorage
        )
      )
    ),
    JtSource(
      "people2",
      SourceDetail.SparkTable(secondSourceCatalog.name, db, table, Some(Iceberg)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail("source2-people", "extracted", JtBookmarkDate, Some("yyyy-MM-dd")),
          jtBookmarkStorage
        )
      )
    )
  )

  private val jtDestination: JtDestination = JtDestination(
    "destination",
    DestinationDetail.SparkTable(
      destinationCatalog.name,
      db,
      table,
      MergeIntoById,
      None,
      Seq("id"),
      Some(Iceberg),
      icebertTableProperties,
      Map.empty,
      Seq("extracted")
    )
  )

  private val jtBadDestination: JtDestination = JtDestination(
    "bad-destination",
    DestinationDetail.SparkTable(
      destinationCatalog.name,
      db,
      badTable,
      MergeIntoById,
      None,
      Seq("id"),
      Some(Iceberg),
      icebertTableProperties,
      Map.empty,
      Seq("extracted")
    )
  )

  private val reportDestination: JtDestination = JtDestination(
    "bad-destination",
    DestinationDetail.SparkTable(
      destinationCatalog.name,
      db,
      reportTable,
      Append,
      None,
      Seq("id"),
      Some(Iceberg),
      icebertTableProperties
    )
  )

  private val jtConfiguration =
    JtEtlDQJobConfiguration(
      jtSources,
      jtDestination,
      jtBadDestination,
      None,
      "it.jobtech.graphenj.core.strategy.PeopleEtlStrategy",
      "it.jobtech.graphenj.core.strategy.DqPeopleStrategy"
    )

  private val jtConfigurationWithReport =
    JtEtlDQJobConfiguration(
      jtSources,
      jtDestination,
      jtBadDestination,
      Some(reportDestination),
      "it.jobtech.graphenj.core.strategy.PeopleEtlStrategy",
      "it.jobtech.graphenj.core.strategy.DqPeopleStrategy"
    )

  override protected def beforeEach(): Unit = {
    ss.close()
    ss = Utils.createSparkSessionWithIceberg(catalogs)
  }

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testPath)
  }

  test("process all without an existing bookmark table") {
    val peopleT1Df = ss
      .createDataFrame(peopleT1)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(firstSourceTablePath, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(secondSourceTablePath, ss.createDataFrame(peopleT2), partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtEtlDqJob(jtConfiguration, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None).run()

    val res               = ss.read.table(destinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = peopleT1 ++ peopleT2
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks =
      Array(JtBookmark("source1-people", "2023-01-08", 999L), JtBookmark("source2-people", "2023-01-06", 999L))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
  }

  test("process all with an existing bookmark table") {
    val bookmarkDate1 = JtBookmark[String]("source1-people", "2023-01-06", 0)
    val bookmarkDate2 = JtBookmark[String]("source2-people", "2023-01-05", 0)
    val bookmarksDf   = ss.createDataFrame(Seq(bookmarkDate1, bookmarkDate2))
    TablesFsUtils.createIcebergTable(bookmarksTablePath, bookmarksDf, Seq.empty, false)
    import bookmarksDf.sparkSession.implicits._
    // writing source tables
    val people1Df     = ss
      .createDataFrame(peopleT1)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    require(
      TablesFsUtils
        .createIcebergTable(firstSourceTablePath, people1Df, partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(secondSourceTablePath, ss.createDataFrame(peopleT2), partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtEtlDqJob(jtConfiguration, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None).run()

    val res               = ss.read.table(destinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       =
      peopleT1.filter(x => x.extracted != "2023-01-05" && x.extracted != "2023-01-06") ++ (peopleT2.filter(x =>
        x.extracted != "2023-01-05"
      ))
    res shouldBe expectedRes
    val bookmarks         =
      ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(x => (x.entry, x.timeMillis))
    val expectedBookmarks =
      Array(
        JtBookmark("source1-people", "2023-01-06", 0),
        JtBookmark("source1-people", "2023-01-08", 999L),
        JtBookmark("source2-people", "2023-01-05", 0),
        JtBookmark("source2-people", "2023-01-06", 999L)
      )
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
  }

  test("process without a source table") {
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(secondSourceTablePath, ss.createDataFrame(peopleT2), partitionKeys, false)
        .isSuccess
    )
    // job running
    val res  =
      Try(new JtEtlDqJob(jtConfiguration, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None).run())
    res.isFailure shouldBe true
    val left = res.toEither.left.get
    left.isInstanceOf[ReadSourceError] shouldBe true
    left.getMessage shouldBe "Unable to read source: SparkTable(source1_catalog,db,people,Some(iceberg))"
    SparkUtils.tableExists(bookmarksCatalog.name, db, bookmarksTable) shouldBe false
  }

  test("process with an error during the writing") {
    val jtWrongDestination: JtDestination = JtDestination(
      "destination",
      DestinationDetail.SparkTable(
        firstSourceCatalog.name,
        db,
        table,
        ErrorIfExists,
        None,
        Seq("id"),
        Some(Iceberg),
        icebertTableProperties,
        Map.empty,
        Seq("extracted")
      )
    )

    val jtWrongConfiguration =
      JtEtlDQJobConfiguration(
        jtSources,
        jtWrongDestination,
        jtBadDestination,
        None,
        "it.jobtech.graphenj.core.strategy.PeopleEtlStrategy",
        "it.jobtech.graphenj.core.strategy.PeopleDqEtlDqTestStrategy"
      )

    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(firstSourceTablePath, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(secondSourceTablePath, ss.createDataFrame(peopleT2), partitionKeys, false)
        .isSuccess
    )
    // job running
    val res  = Try(
      new JtEtlDqJob(jtWrongConfiguration, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None).run()
    )
    res.isFailure shouldBe true
    val left = res.toEither.left.get
    left.isInstanceOf[WriteError] shouldBe true
    left.getMessage shouldBe "Unable to write data to the destination: SparkTable(source1_catalog,db,people," +
      "ErrorIfExists,None,List(id),Some(iceberg),Map(write.format.default -> parquet, format-version -> 2, " +
      "write.parquet.compression-codec -> snappy),Map(),List(extracted))"
    ss.table(bookmarksTablePath).count() shouldBe 0
  }

  test("process with an error during the writing with bookmarks at the start") {
    val bookmarkDate1 = JtBookmark[String]("source1-people", "2023-01-06", 0)
    val bookmarkDate2 = JtBookmark[String]("source2-people", "2023-01-05", 0)
    val bookmarksDf   = ss.createDataFrame(Seq(bookmarkDate1, bookmarkDate2))
    TablesFsUtils.createIcebergTable(bookmarksTablePath, bookmarksDf, Seq.empty, false)

    import bookmarksDf.sparkSession.implicits._

    val jtWrongDestination: JtDestination = JtDestination(
      "destination",
      DestinationDetail.SparkTable(
        firstSourceCatalog.name,
        db,
        table,
        ErrorIfExists,
        None,
        Seq("id"),
        Some(Iceberg),
        icebertTableProperties,
        Map.empty,
        Seq("extracted")
      )
    )

    val jtWrongConfiguration =
      JtEtlDQJobConfiguration(
        jtSources,
        jtWrongDestination,
        jtBadDestination,
        None,
        "it.jobtech.graphenj.core.strategy.PeopleEtlStrategy",
        "it.jobtech.graphenj.core.strategy.PeopleDqEtlDqTestStrategy"
      )

    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(firstSourceTablePath, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(secondSourceTablePath, ss.createDataFrame(peopleT2), partitionKeys, false)
        .isSuccess
    )
    // job running
    val res               = Try(
      new JtEtlDqJob(jtWrongConfiguration, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None).run()
    )
    res.isFailure shouldBe true
    val left              = res.toEither.left.get
    left.isInstanceOf[WriteError] shouldBe true
    left.getMessage shouldBe "Unable to write data to the destination: SparkTable(source1_catalog,db,people," +
      "ErrorIfExists,None,List(id),Some(iceberg),Map(write.format.default -> parquet, format-version -> 2, " +
      "write.parquet.compression-codec -> snappy),Map(),List(extracted))"
    val bookmarks         =
      ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(x => (x.entry, x.timeMillis))
    val expectedBookmarks =
      Array(JtBookmark("source1-people", "2023-01-06", 0), JtBookmark("source2-people", "2023-01-05", 0))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
  }

  test("process all without an existing bookmark table failing the data quality checking test") {
    val duplicatedPerson = Person(1, "harry", "potter", 1672918958, "2023-01-05")
    val peopleT1Df       = ss
      .createDataFrame(peopleT1)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(firstSourceTablePath, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(
          secondSourceTablePath,
          ss.createDataFrame(peopleT2 :+ duplicatedPerson),
          partitionKeys,
          false
        )
        .isSuccess
    )
    // job running
    new JtEtlDqJob(jtConfigurationWithReport, new PeopleEtlStrategy, new PeopleDqEtlDqTestStrategy, None, None, None)
      .run()

    val res               = ss.read.table(badDestinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = (peopleT1 ++ peopleT2 :+ duplicatedPerson).sortBy(_.id)
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks =
      Array(JtBookmark("source1-people", "2023-01-08", 999L), JtBookmark("source2-people", "2023-01-06", 999L))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
    val report            = ss.read.table(reportDestinationTablePath).as[DeequReport].collect()
    val expectedReport    = Array(
      DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Error,
        "CompletenessConstraint(Completeness(firstName,None))",
        ConstraintStatus.Success,
        None
      ),
      DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Error,
        "CompletenessConstraint(Completeness(lastName,None))",
        ConstraintStatus.Success,
        None
      ),
      DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Error,
        "UniquenessConstraint(Uniqueness(List(id),None))",
        ConstraintStatus.Failure,
        Some("Value: 0.7142857142857143 does not meet the constraint requirement!")
      )
    )
    report shouldBe expectedReport
  }

}
