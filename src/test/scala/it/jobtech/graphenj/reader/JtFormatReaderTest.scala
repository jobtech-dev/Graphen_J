package it.jobtech.graphenj.reader

import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model.SourceDetail.Format
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.configuration.model.bookmark._
import it.jobtech.graphenj.models.JtBookmark
import it.jobtech.graphenj.utils.JtError.ReadSourceError
import it.jobtech.graphenj.utils.models.{ Person, SparkCatalogInfo }
import it.jobtech.graphenj.utils.{ TablesFsUtils, Utils }
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JtFormatReaderTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val testPath           = "tmp"
  private val db                 = "sourceDb"
  private val table              = "people"
  private val bookmarksTable     = "bookmarks"
  private val catalog            = SparkCatalogInfo("source_catalog", "tmp/")
  private val tablePath          = s"$testPath/$table"
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

  test("non existent path") {
    val res = new JtFormatReader().read(Format("parquet", None, Map("path" -> tablePath)), None)

    res.isLeft shouldBe true
    val left = res.left.get
    left.isInstanceOf[ReadSourceError] shouldBe true
    left.source shouldBe Format("parquet", None, Map("path" -> tablePath))
    left.throwable.isInstanceOf[AnalysisException] shouldBe true
    left.throwable.getMessage.contains("Path does not exist") shouldBe true
  }

  test("read parquet file correctly without bookmark") {
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.parquet(tablePath)
    val res = new JtFormatReader().read(Format("parquet", None, Map("path" -> tablePath)), None)

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("read partitioned format correctly without bookmark") {
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.partitionBy(nameOf[Person](_.extracted)).parquet(tablePath)
    val res = new JtFormatReader().read(Format("parquet", None, Map("path" -> tablePath)), None)

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("read partitioned format with emtpy bookmarks") {
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.partitionBy(nameOf[Person](_.extracted)).parquet(tablePath)
    val res = new JtFormatReader().read(
      Format("parquet", None, Map("path" -> tablePath)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.extracted), JtBookmarkDate, Some(datePattern)),
          bookmarkStorage
        )
      )
    )

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("read format with bookmarks date type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkDate)), Seq.empty, false)
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.partitionBy(nameOf[Person](_.extracted)).parquet(tablePath)
    val res = new JtFormatReader().read(
      Format("parquet", None, Map("path" -> tablePath)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.extracted), JtBookmarkDate, Some(datePattern)),
          bookmarkStorage
        )
      )
    )

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.extracted == "2023-01-06")
  }

  test("read format with bookmarks int type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkInt)), Seq.empty, false)
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.partitionBy(nameOf[Person](_.updatedAt)).parquet(tablePath)
    val res = new JtFormatReader().read(
      Format("parquet", None, Map("path" -> tablePath)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.updatedAt), JtBookmarkInt, Some(datePattern)),
          bookmarkStorage
        )
      )
    )

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.updatedAt > bookmarkInt.lastRead)
  }

  test("read format with bookmarks long type") {
    TablesFsUtils.createIcebergTable(bookmarksTablePath, ss.createDataFrame(Seq(bookmarkLong)), Seq.empty, false)
    val df  = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    df.as[Person].write.partitionBy(nameOf[Person](_.updatedAt)).parquet(tablePath)
    val res = new JtFormatReader().read(
      Format("parquet", None, Map("path" -> tablePath)),
      Some(
        JtBookmarksConf(
          JtBookmarkDetail(table, nameOf[Person](_.updatedAt), JtBookmarkLong, Some(datePattern)),
          bookmarkStorage
        )
      )
    )

    res.isRight shouldBe true
    res.right.get.as[Person].collect().sortBy(_.id) shouldBe people.filter(x => x.updatedAt > bookmarkLong.lastRead)
  }
}
