package it.jobtech.graphenj.writer

import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model.DestinationDetail.Format
import it.jobtech.graphenj.utils.Utils
import it.jobtech.graphenj.utils.models.Person
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{ Files, Paths }

class JtFormatWriterTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {
  private val testWritingPath        = "tmp"
  implicit lazy val ss: SparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  private val people = Seq(
    Person(1, "harry", "potter", 1672918958, "2023-01-05"),
    Person(2, "jack", "sparrow", 1672918958, "2023-01-05"),
    Person(3, "albus", "silente", 1673005358, "2023-01-06"),
    Person(4, "ace", "ventura", 1673005359, "2023-01-06")
  )

  import ss.implicits._

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testWritingPath)
  }

  test("write parquet correctly without partitioning") {
    val df                = ss.createDataFrame(people)
    val destinationDetail =
      Format("parquet", Map("compression" -> "snappy", "path" -> testWritingPath), SaveMode.Append, Seq.empty)
    val res               = new JtFormatWriter().write(df, destinationDetail)

    res.isRight shouldBe true
    ss.read.parquet(testWritingPath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write orc correctly without partitioning") {
    val df                = ss.createDataFrame(people)
    val destinationDetail =
      Format("orc", Map("path" -> testWritingPath), SaveMode.Overwrite, Seq.empty)
    val res               = new JtFormatWriter().write(df, destinationDetail)

    res.isRight shouldBe true
    ss.read
      .orc(testWritingPath)
      .as[Person]
      .collect()
      .sortBy(_.id) shouldBe people
  }

  test("write parquet correctly with partitioning") {
    val df                = ss.createDataFrame(people)
    val destinationDetail =
      Format(
        "parquet",
        Map("compression" -> "snappy", "path" -> testWritingPath),
        SaveMode.Append,
        Seq(nameOf[Person](_.extracted))
      )
    val res               = new JtFormatWriter().write(df, destinationDetail)

    res.isRight shouldBe true
    ss.read.parquet(testWritingPath).as[Person].collect().sortBy(_.id) shouldBe people
    Files.exists(Paths.get(s"$testWritingPath/extracted=2023-01-05")) shouldBe true
    Files.exists(Paths.get(s"$testWritingPath/extracted=2023-01-06")) shouldBe true
  }
}
