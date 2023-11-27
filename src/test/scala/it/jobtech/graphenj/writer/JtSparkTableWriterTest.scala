package it.jobtech.graphenj.writer

import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.configuration.model.DestinationDetail.SparkTable
import it.jobtech.graphenj.configuration.model._
import it.jobtech.graphenj.utils.JtError.WriteError
import it.jobtech.graphenj.utils.Utils
import it.jobtech.graphenj.utils.models.{ Person, SparkCatalogInfo }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{ Files, Paths }
import scala.util.{ Failure, Success, Try }

class JtSparkTableWriterTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val testPath      = "tmp"
  private val db            = "sourceDb"
  private val table         = "people"
  private val partitionKeys = Seq("extracted")
  private val catalog       = SparkCatalogInfo("source_catalog", "tmp/")
  private val tablePath     = s"${catalog.name}.$db.$table"

  implicit private var ss: SparkSession = Utils.createSparkSessionWithIceberg(Seq(catalog))

  private val people = Seq(
    Person(1, "harry", "potter", 1672918958, "2023-01-05"),
    Person(2, "jack", "sparrow", 1672918958, "2023-01-05"),
    Person(3, "albus", "silente", 1673005358, "2023-01-06"),
    Person(4, "ace", "ventura", 1673005359, "2023-01-06")
  )

  private val integrationTableFunction = (df: DataFrame, dest: SparkTable) => {
    df.createOrReplaceTempView("my_view")
    Try(ss.sql(sqlText = s"INSERT INTO $tablePath (SELECT * FROM my_view)")) match {
      case Failure(exception) => Left(WriteError(dest, exception))
      case Success(_)         => Right(())
    }
  }

  override protected def beforeEach(): Unit = {
    ss.close()
    ss = Utils.createSparkSessionWithIceberg(Seq(catalog))
  }

  override protected def afterEach(): Unit = {
    Utils.deleteResDirectory(testPath)
  }

  test("write table without partitioning in ErrorIfExists mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val res             =
      new JtSparkTableWriter()
        .write(
          df,
          SparkTable(
            catalog.name,
            db,
            table,
            ErrorIfExists,
            None,
            Seq.empty,
            Some(Iceberg),
            tableProperties,
            Map.empty,
            Seq.empty
          )
        )

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table with partitioning in ErrorIfExists mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val res             =
      new JtSparkTableWriter()
        .write(
          df,
          SparkTable(
            catalog.name,
            db,
            table,
            ErrorIfExists,
            None,
            Seq.empty,
            Some(Iceberg),
            tableProperties,
            Map.empty,
            partitionKeys
          )
        )

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
    Files.exists(Paths.get("%s/%s/%s/data/extracted=2023-01-05".format(testPath, db, table))) shouldBe true
    Files.exists(Paths.get("%s/%s/%s/data/extracted=2023-01-06".format(testPath, db, table))) shouldBe true
  }

  test("write table without partitioning in ErrorIfExists mode with an existent table") {
    val df: DataFrame   = ss.createDataFrame(people)
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            = SparkTable(
      catalog.name,
      db,
      table,
      ErrorIfExists,
      None,
      Seq.empty,
      Some(Iceberg),
      tableProperties,
      Map.empty,
      Seq.empty
    )
    val res             = new JtSparkTableWriter().write(df, dest)

    res.isLeft shouldBe true
    val left = res.left.get
    left.dest shouldBe dest
    left.throwable.isInstanceOf[UnsupportedOperationException] shouldBe true
    left.throwable.getMessage shouldBe s"Error writing data with $ErrorIfExists mode: table already exists!"
  }

  test("write table in Overwrite mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val res             =
      new JtSparkTableWriter()
        .write(
          df,
          SparkTable(
            catalog.name,
            db,
            table,
            Overwrite,
            None,
            Seq.empty,
            Some(Iceberg),
            tableProperties,
            Map.empty,
            Seq.empty
          )
        )

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in Overwrite mode with an existent table") {
    val df: DataFrame   = ss.createDataFrame(Seq(Person(7, "to", "override", 0, "2023-01-06")))
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            = SparkTable(
      catalog.name,
      db,
      table,
      Overwrite,
      None,
      Seq.empty,
      Some(Iceberg),
      tableProperties,
      Map.empty,
      Seq.empty
    )
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in Append mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val res             =
      new JtSparkTableWriter()
        .write(
          df,
          SparkTable(
            catalog.name,
            db,
            table,
            Append,
            None,
            Seq.empty,
            Some(Iceberg),
            tableProperties,
            Map.empty,
            Seq.empty
          )
        )

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in Append mode with an existent table") {
    val firstRows       = Seq(Person(0, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(catalog.name, db, table, Append, None, Seq.empty, Some(Iceberg), tableProperties, Map.empty, Seq.empty)
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe firstRows ++ people
  }

  test("write table in AppendOnlyNewRows mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        AppendOnlyNewRows,
        None,
        Seq(nameOf[Person](_.id)),
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(df, dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in AppendOnlyNewRows mode with an existent table") {
    val firstRows       = Seq(Person(1, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        AppendOnlyNewRows,
        None,
        Seq(nameOf[Person](_.id)),
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    val expectedPeople = Seq(
      Person(1, "first", "person", 0, "2023-01-05"),
      Person(2, "jack", "sparrow", 1672918958, "2023-01-05"),
      Person(3, "albus", "silente", 1673005358, "2023-01-06"),
      Person(4, "ace", "ventura", 1673005359, "2023-01-06")
    )
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe expectedPeople
  }

  test("write table in AppendOnlyNewRows mode with an existent table and different id") {
    val idFields        = Seq(nameOf[Person](_.id), nameOf[Person](_.updatedAt))
    val firstRows       = Seq(Person(1, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        AppendOnlyNewRows,
        None,
        idFields,
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].sort(idFields.map(col): _*).collect() shouldBe firstRows ++ people
  }

  test("write table in MergeIntoById mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        MergeIntoById,
        None,
        Seq(nameOf[Person](_.id)),
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(df, dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in MergeIntoById mode with an existent table") {
    val firstRows       = Seq(Person(1, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        MergeIntoById,
        None,
        Seq(nameOf[Person](_.id)),
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in MergeIntoById mode with an existent table and different id") {
    val idFields        = Seq(nameOf[Person](_.id), nameOf[Person](_.updatedAt))
    val firstRows       = Seq(Person(1, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            =
      SparkTable(
        catalog.name,
        db,
        table,
        MergeIntoById,
        None,
        idFields,
        Some(Iceberg),
        tableProperties,
        Map.empty,
        Seq.empty
      )
    val res             = new JtSparkTableWriter().write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].sort(idFields.map(col): _*).collect() shouldBe firstRows ++ people
  }

  test("write table in Custom mode") {
    val df: DataFrame   = ss.createDataFrame(people)
    import df.sparkSession.implicits._
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            = SparkTable(
      catalog.name,
      db,
      table,
      Custom,
      Some("f"),
      Seq.empty,
      Some(Iceberg),
      tableProperties,
      Map.empty,
      Seq.empty
    )
    val res             = new JtSparkTableWriter(Some(integrationTableFunction)).write(df, dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe people
  }

  test("write table in Custom mode with an existent table") {
    val firstRows       = Seq(Person(0, "first", "person", 0, "2023-01-05"))
    val df: DataFrame   = ss.createDataFrame(firstRows)
    import df.sparkSession.implicits._
    df.writeTo(tablePath).create()
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            = SparkTable(
      catalog.name,
      db,
      table,
      Custom,
      Some("f"),
      Seq.empty,
      Some(Iceberg),
      tableProperties,
      Map.empty,
      Seq.empty
    )
    val res             = new JtSparkTableWriter(Some(integrationTableFunction)).write(ss.createDataFrame(people), dest)

    res.isRight shouldBe true
    ss.table(tablePath).as[Person].collect().sortBy(_.id) shouldBe firstRows ++ people
  }

  test("write table in Custom mode without passing an integrationTableFunction") {
    val df: DataFrame   = ss.createDataFrame(people)
    val tableProperties =
      Map("write.format.default" -> "parquet", "format-version" -> "2", "write.parquet.compression-codec" -> "snappy")
    val dest            = SparkTable(
      catalog.name,
      db,
      table,
      Custom,
      Some("f"),
      Seq.empty,
      Some(Iceberg),
      tableProperties,
      Map.empty,
      Seq.empty
    )
    val res             = new JtSparkTableWriter().write(df, dest)

    res.isLeft shouldBe true
    val left            = res.left.get
    left.throwable.isInstanceOf[IllegalArgumentException] shouldBe true
    val expectedMessage = s"Error writing data with $Custom mode: the function tableIntegrationFunction is missing!"
    left.throwable.getMessage shouldBe expectedMessage
  }

}
