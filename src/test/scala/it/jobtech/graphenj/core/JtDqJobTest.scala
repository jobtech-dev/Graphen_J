package it.jobtech.graphenj.core

import com.amazon.deequ.checks.{ CheckLevel, CheckStatus }
import com.amazon.deequ.constraints.ConstraintStatus
import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model.JtJobConfiguration.JtDQJobConfiguration
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  FS,
  JtBookmarkDate,
  JtBookmarkStorage,
  JtBookmarkStorageDetail
}
import it.jobtech.graphenj.core.models.DeequReport
import it.jobtech.graphenj.core.strategy.PeopleDqStrategy
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.JtError.WriteError
import it.jobtech.graphenj.utils.models.{ Person, SparkCatalogInfo }
import it.jobtech.graphenj.utils.{ AssertEqualsIgnoringFields, TablesFsUtils, Utils }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, to_date }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import scala.util.Try

class JtDqJobTest extends AnyFunSuite with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testPath           = "tmp"
  private val db                 = "db"
  private val table              = "people"
  private val badTable           = "bad_people"
  private val reportTable        = "report_people"
  private val bookmarksTable     = "bookmarks"
  private val partitionKeys      = Seq("extracted")
  private val sourceCatalog      = SparkCatalogInfo("source1_catalog", "tmp/source1")
  private val destinationCatalog = SparkCatalogInfo("destination_catalog", "tmp/destination")
  private val bookmarksCatalog   = SparkCatalogInfo("bookmarks_catalog", "tmp/bookmarks")
  private val catalogs           = Seq(sourceCatalog, destinationCatalog, bookmarksCatalog)

  private val sourceTable                = s"${sourceCatalog.name}.$db.$table"
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

  private val icebergTableProperties: Map[String, String] =
    Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")

  private val jtBookmarkStorage: JtBookmarkStorage =
    JtBookmarkStorage(
      "bookmarkStorage",
      FS,
      JtBookmarkStorageDetail.SparkTable(bookmarksCatalog.name, db, "bookmarks", Some(Iceberg), icebergTableProperties)
    )

  private val jtSource: JtSource =
    JtSource(
      "people1",
      SourceDetail.SparkTable(sourceCatalog.name, db, table, Some(Iceberg)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail("source1-people", "extracted", JtBookmarkDate, Some("yyyy-MM-dd")),
          jtBookmarkStorage
        )
      )
    )

  private val jtDestination: JtDestination = JtDestination(
    "good-destination",
    DestinationDetail.SparkTable(
      destinationCatalog.name,
      db,
      table,
      ErrorIfExists,
      None,
      Seq.empty,
      Some(Iceberg),
      icebergTableProperties,
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
      icebergTableProperties,
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
      icebergTableProperties
    )
  )

  private val jtConfiguration =
    JtDQJobConfiguration(
      jtSource,
      jtDestination,
      jtBadDestination,
      None,
      "it.jobtech.graphenj.core.strategy.DqPeopleStrategy"
    )

  private val jtConfigurationWithReport =
    JtDQJobConfiguration(
      jtSource,
      jtDestination,
      jtBadDestination,
      Some(reportDestination),
      "it.jobtech.graphenj.core.strategy.DqPeopleStrategy"
    )

  override protected def beforeEach(): Unit = {
    ss.close()
    ss = Utils.createSparkSessionWithIceberg(catalogs)
  }

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testPath)
  }

  test("process good data without an existing bookmark table not saving the report") {
    val peopleT1Df = ss
      .createDataFrame(peopleT1)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(sourceTable, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtDqJob(jtConfiguration, new PeopleDqStrategy, None, None, None).run()

    val res               = ss.read.table(destinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = peopleT1
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks = Array(JtBookmark("source1-people", "2023-01-08", 999L))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
  }

  test("process good data without an existing bookmark table saving the report") {
    val peopleT1Df = ss
      .createDataFrame(peopleT1)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(sourceTable, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtDqJob(jtConfigurationWithReport, new PeopleDqStrategy, None, None, None).run()

    val res               = ss.read.table(destinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = peopleT1
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks = Array(JtBookmark("source1-people", "2023-01-08", 999L))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )

    val report         = ss.read.table(reportDestinationTablePath).as[DeequReport].collect()
    val expectedReport = Array(
      models.DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Success,
        "SizeConstraint(Size(None))",
        ConstraintStatus.Success,
        None
      ),
      models.DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Success,
        "CompletenessConstraint(Completeness(firstName,None))",
        ConstraintStatus.Success,
        None
      ),
      models.DeequReport(
        "Person test dataset",
        CheckLevel.Error,
        CheckStatus.Success,
        "CompletenessConstraint(Completeness(lastName,None))",
        ConstraintStatus.Success,
        None
      )
    )
    report shouldBe expectedReport
  }

  test("process bad data without an existing bookmark table not saving the report") {
    val peopleT1Df = ss
      .createDataFrame(peopleT1.tail)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(sourceTable, peopleT1Df, partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtDqJob(jtConfiguration, new PeopleDqStrategy, None, None, None).run()

    val res               = ss.read.table(badDestinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = peopleT1.tail
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks = Array(JtBookmark("source1-people", "2023-01-08", 999L))
    AssertEqualsIgnoringFields.assertEqualsCollectionIgnoringFields(
      bookmarks,
      expectedBookmarks,
      Seq(nameOf[JtBookmark[_]](_.timeMillis))
    )
  }

  test("process bad data without an existing bookmark table saving the report") {
    val peopleT1Df = ss
      .createDataFrame(peopleT1.tail)
      .withColumn(nameOf[Person](_.extracted), to_date(col(nameOf[Person](_.extracted)), "yyyy-MM-dd"))
    import peopleT1Df.sparkSession.implicits._
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(sourceTable, peopleT1Df, partitionKeys, false)
        .isSuccess
    )
    // job running
    new JtDqJob(jtConfigurationWithReport, new PeopleDqStrategy, None, None, None).run()

    val res               = ss.read.table(badDestinationTablePath).as[Person].collect().sortBy(_.id)
    val expectedRes       = peopleT1.tail
    res shouldBe expectedRes
    val bookmarks         = ss.read.table((bookmarksTablePath)).as[JtBookmark[String]].collect().sortBy(_.entry)
    val expectedBookmarks = Array(JtBookmark("source1-people", "2023-01-08", 999L))
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
        "SizeConstraint(Size(None))",
        ConstraintStatus.Failure,
        Some("Value: 3 does not meet the constraint requirement!")
      ),
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
      )
    )
    report shouldBe expectedReport
  }

  test("Error during the good data saving") {
    // writing source tables
    require(
      TablesFsUtils
        .createIcebergTable(sourceTable, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    require(
      TablesFsUtils
        .createIcebergTable(destinationTablePath, ss.createDataFrame(peopleT1), partitionKeys, false)
        .isSuccess
    )
    // job running
    val res       = Try(new JtDqJob(jtConfiguration, new PeopleDqStrategy, None, None, None).run())
    res.isFailure shouldBe true
    res.failed.get.isInstanceOf[WriteError] shouldBe true
    val exception = res.failed.get.asInstanceOf[WriteError]
    exception.dest shouldBe jtDestination.detail
    exception.message shouldBe "Unable to write data to the destination: SparkTable(destination_catalog,db,people," +
      "ErrorIfExists,None,List(),Some(iceberg),Map(write.format.default -> parquet, format-version -> 2, " +
      "write.parquet.compression-codec -> snappy),Map(),List(extracted))"
    ss.read.table((bookmarksTablePath)).count() shouldBe 0
  }
}
