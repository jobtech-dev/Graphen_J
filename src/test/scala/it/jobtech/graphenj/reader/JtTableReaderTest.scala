package it.jobtech.graphenj.reader

import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark.{
  JtBookmarkDate,
  JtBookmarkInt,
  JtBookmarkLong,
  JtBookmarkStorage,
  JtBookmarkStorageDetail,
  OS
}
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import it.jobtech.graphenj.utils.models.{ Person, SparkCatalogInfo }
import it.jobtech.graphenj.utils.{ TablesFsUtils, Utils }
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JtTableReaderTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val testPath           = "tmp"
  private val db                 = "sourceDb"
  private val table              = "people"
  private val bookmarksTable     = "bookmarks"
  private val partitionKeys      = Seq("extracted")
  private val catalog            = SparkCatalogInfo("source_catalog", "tmp/")
  private val tablePath          = s"${catalog.name}.$db.$table"
  private val bookmarksTablePath = s"${catalog.name}.$db.$bookmarksTable"

  private val datePattern = "yyyy-MM-dd"

  implicit private var ss: SparkSession = Utils.createSparkSessionWithIceberg(Seq(catalog))

  private val people = Seq(
    Person(1, "harry", "potter", 1672918958, "2023-01-05"),
    Person(2, "jack", "sparrow", 1672918958, "2023-01-05"),
    Person(3, "albus", "silente", 1673005358, "2023-01-06"),
    Person(4, "ace", "ventura", 1673005359, "2023-01-06")
  )

  private val bookmarkStorage = JtBookmarkStorage(
    "bookmark",
    OS,
    JtBookmarkStorageDetail.SparkTable(catalog.name, db, bookmarksTable, None, Map.empty)
  )

  private val bookmarkDate = JtBookmark[String](table, "2023-01-05", 0)
  private val bookmarkInt  = JtBookmark[Int](table, 1672918958, 0)
  private val bookmarkLong = JtBookmark[Long](table, 1672918958L, 0)

  override protected def beforeEach(): Unit = {
    ss.close()
    ss = Utils.createSparkSessionWithIceberg(Seq(catalog))
  }

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testPath)
  }

  test("non existent table") {
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    val r      = new JtTableReader()
    val res    = r.read(source, None)
    res.isLeft shouldBe true
    val left   = res.left.get
    left.isInstanceOf[ReadSourceError] shouldBe true
    left.throwable.isInstanceOf[AnalysisException] shouldBe true
    left.message shouldBe "Unable to read source: SparkTable(source_catalog,sourceDb,people,None)"
  }

  test("read all without bookmarks") {
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    TablesFsUtils.createIcebergTable(tablePath, ss.createDataFrame(people), partitionKeys, false)
    val r      = new JtTableReader()
    val res    = r.read(source, None)
    res.isRight shouldBe true
    val df     = res.right.get
    import df.sparkSession.implicits._
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("read all with bookmarks") {
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    TablesFsUtils.createIcebergTable(tablePath, ss.createDataFrame(people), partitionKeys, false)
    val res    = new JtTableReader().read(
      source,
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.extracted), JtBookmarkDate, Some(datePattern)),
          bookmarkStorage
        )
      )
    )
    res.isRight shouldBe true
    val df     = res.right.get
    import df.sparkSession.implicits._
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("read with bookmarks date type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkDate)), Seq.empty, false)
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    TablesFsUtils.createIcebergTable(tablePath, ss.createDataFrame(people), partitionKeys, false)
    val res    = new JtTableReader().read(
      source,
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.extracted), JtBookmarkDate, Some(datePattern)),
          bookmarkStorage
        )
      )
    )
    res.isRight shouldBe true
    val df     = res.right.get
    import df.sparkSession.implicits._
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.extracted == "2023-01-06")
  }

  test("read with bookmarks int type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkInt)), Seq.empty, false)
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    TablesFsUtils.createIcebergTable(tablePath, ss.createDataFrame(people), partitionKeys, false)
    val res    = new JtTableReader().read(
      source,
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.updatedAt), JtBookmarkInt, Some(datePattern)),
          bookmarkStorage
        )
      )
    )
    res.isRight shouldBe true
    val df     = res.right.get
    import df.sparkSession.implicits._
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.updatedAt > bookmarkInt.lastRead)
  }

  test("read with bookmarks long type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkLong)), Seq.empty, false)
    val source = SourceDetail.SparkTable(catalog.name, db, table, None)
    TablesFsUtils.createIcebergTable(tablePath, ss.createDataFrame(people), partitionKeys, false)
    val res    = new JtTableReader().read(
      source,
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.updatedAt), JtBookmarkLong, Some(datePattern)),
          bookmarkStorage
        )
      )
    )
    res.isRight shouldBe true
    val df     = res.right.get
    import df.sparkSession.implicits._
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.updatedAt > bookmarkLong.lastRead)
  }
}
