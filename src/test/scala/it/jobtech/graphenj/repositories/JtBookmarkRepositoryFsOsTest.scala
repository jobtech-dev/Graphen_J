package it.jobtech.graphenj.repositories

import it.jobtech.graphenj.configuration.model.bookmark.JtBookmarkStorageDetail
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.models.SparkCatalogInfo
import it.jobtech.graphenj.utils.{ TablesFsUtils, Utils }
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JtBookmarkRepositoryFsOsTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val testPath  = "tmp"
  private val db        = "db"
  private val table     = "bookmarks"
  private val catalog   = SparkCatalogInfo("bookmarks_catalog", "tmp/")
  private val tablePath = s"${catalog.name}.$db.$table"

  private val storageDetail = JtBookmarkStorageDetail.SparkTable(catalog.name, db, table, None, Map.empty)

  implicit var ss: SparkSession = Utils.createSparkSessionWithIceberg(Seq(catalog))

  private val b1 = JtBookmark("table1", "2023-01-02", 0)
  private val b2 = JtBookmark("table1", "2023-01-03", 1)
  private val b3 = JtBookmark("table2", "2023-01-04", 3)

  private val bookmarks = Seq(b1, b2, b3)

  override protected def beforeEach(): Unit = {
    ss.close()
    ss = Utils.createSparkSessionWithIceberg(Seq(catalog))
  }

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testPath)
  }

  test("read non existent bookmark table") {
    val repo = new JtBookmarkRepositoryFsOs(storageDetail)
    repo.getLastBookmark[String]("table1") shouldBe Right(None)
  }

  test("read non existent bookmark entry") {
    val df   = ss.createDataFrame(bookmarks)
    val repo = new JtBookmarkRepositoryFsOs(storageDetail)
    TablesFsUtils.createIcebergTable(tablePath, df, Seq.empty, false)
    repo.getLastBookmark[String]("fakeTable") shouldBe Right(None)
  }

  test("read the bookmark entry which exists") {
    val df   = ss.createDataFrame(bookmarks)
    val repo = new JtBookmarkRepositoryFsOs(storageDetail)
    require(TablesFsUtils.createIcebergTable(tablePath, df, Seq.empty, false).isSuccess)
    repo.getLastBookmark[String]("table2") shouldBe Right(Some(b3))
  }

  test("read with the existence of multiple bookmark entries") {
    val df   = ss.createDataFrame(bookmarks)
    val repo = new JtBookmarkRepositoryFsOs(storageDetail)
    require(TablesFsUtils.createIcebergTable(tablePath, df, Seq.empty, false).isSuccess)
    repo.getLastBookmark[String]("table1") shouldBe Right(Some(b2))
  }

  test("write the bookmark entry with non existent table") {
    val repo          = new JtBookmarkRepositoryFsOs(storageDetail)
    repo.writeBookmark("table2", "2023-01-04") shouldBe Right(())
    val df            = ss.table(tablePath)
    import df.sparkSession.implicits._
    val expectedValue = JtBookmark[String]("table2", "2023-01-04", 999)
    val res           = df.as[JtBookmark[String]].collect()
    res.length shouldBe 1
    res.head.entry shouldBe expectedValue.entry
    res.head.lastRead shouldBe expectedValue.lastRead
  }

  test("write the bookmark entry with an existent table") {
    val df              = ss.createDataFrame(bookmarks)
    import df.sparkSession.implicits._
    val repo            = new JtBookmarkRepositoryFsOs(storageDetail)
    require(TablesFsUtils.createIcebergTable(tablePath, df, Seq.empty, false).isSuccess)
    repo.writeBookmark[String]("table2", "2023-01-05") shouldBe Right(())
    val res             = ss.table(tablePath).as[JtBookmark[String]].collect()
    res.length shouldBe 4
    val expectedValue   = JtBookmark[String]("table2", "2023-01-05", -1)
    val writtenBookmark = res.maxBy(_.timeMillis)
    writtenBookmark.entry shouldBe expectedValue.entry
    writtenBookmark.lastRead shouldBe expectedValue.lastRead
  }

  test("delete the last bookmark value for the entry") {
    val df   = ss.createDataFrame(bookmarks)
    val repo = new JtBookmarkRepositoryFsOs(storageDetail)
    require(TablesFsUtils.createIcebergTable(tablePath, df, Seq.empty, false).isSuccess)
    repo.deleteLastBookmark(b1.entry)
    repo.getLastBookmark[String](b1.entry) shouldBe Right(Some(b1))
  }

}
