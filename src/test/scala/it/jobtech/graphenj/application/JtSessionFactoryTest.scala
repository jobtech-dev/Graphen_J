package it.jobtech.graphenj.application

import it.jobtech.graphenj.configuration.model.ApacheIcebergCatalog.{
  GlueApacheIcebergCatalog,
  HadoopApacheIcebergCatalog
}
import it.jobtech.graphenj.configuration.model.JtSession
import it.jobtech.graphenj.configuration.model.SessionType.Local
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JtSessionFactoryTest extends AnyFunSuite with Matchers {

  test("spark session creation without using the SessionFactory") {
    val jtSession      = JtSession(
      Local,
      "test-application",
      None,
      None,
      None
    )
    val sessionFactory = new JtSessionFactory
    val res            = sessionFactory.getOrCreate(jtSession)

    res.isRight shouldBe true
    res.right.get.conf.get("spark.app.name") shouldBe "test-application"
    res.right.get.conf.get("spark.master") shouldBe "local[*]"
    res.right.get.close()
  }

  test("spark session creation with iceberg using the SessionFactory") {
    val jtSession      = JtSession(
      Local,
      "test-application",
      None,
      Some(true),
      Some(
        Seq(
          HadoopApacheIcebergCatalog("source_catalog1", "source_warehouse_path", None),
          HadoopApacheIcebergCatalog("source_catalog2", "source_warehouse_path", Some("endpoint")),
          GlueApacheIcebergCatalog("destination_catalog", "destination_warehouse_path")
        )
      )
    )
    val sessionFactory = new JtSessionFactory
    val res            = sessionFactory.getOrCreate(jtSession)

    val glueCatalog                  = "org.apache.iceberg.aws.glue.GlueCatalog"
    val icebergSparkSessionExtension = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

    res.isRight shouldBe true
    res.right.get.conf.get("spark.sql.catalog.destination_catalog") shouldBe "org.apache.iceberg.spark.SparkCatalog"
    res.right.get.conf.get("spark.sql.catalog.source_catalog1.warehouse") shouldBe "source_warehouse_path"
    res.right.get.conf.get("spark.app.name") shouldBe "test-application"
    res.right.get.conf.get("spark.sql.catalog.destination_catalog.warehouse") shouldBe "destination_warehouse_path"
    res.right.get.conf.get("spark.sql.catalog.destination_catalog.catalog-impl") shouldBe glueCatalog
    res.right.get.conf.get("spark.sql.catalog.source_catalog1") shouldBe "org.apache.iceberg.spark.SparkCatalog"
    res.right.get.conf.get("spark.master") shouldBe "local[*]"
    res.right.get.conf.get("spark.sql.catalog.source_catalog1.type") shouldBe "hadoop"
    res.right.get.conf.get("spark.sql.extensions") shouldBe icebergSparkSessionExtension
    res.right.get.conf.get("spark.sql.catalog.spark_catalog") shouldBe "org.apache.iceberg.spark.SparkSessionCatalog"
    res.right.get.conf.get("spark.sql.catalog.source_catalog2.type") shouldBe "hadoop"
    res.right.get.conf.get("spark.sql.catalog.source_catalog2.warehouse") shouldBe "source_warehouse_path"
    res.right.get.conf.get("spark.sql.catalog.source_catalog2.hadoop.fs.s3a.endpoint") shouldBe "endpoint"
    res.right.get.close()
  }
}
