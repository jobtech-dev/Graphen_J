package it.jobtech.graphenj.utils

import it.jobtech.graphenj.utils.models.SparkCatalogInfo
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.net.URI
import scala.reflect.io.Directory

object Utils {

  def createSparkSessionWithIceberg(catalogInfo: Seq[SparkCatalogInfo]): SparkSession = {
    val sparkConfTmp = new SparkConf()
      .setAppName("testETL")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.eventLog.enabled", "false")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.warehouse.dir", "tmp/warehouse-dir")

    val sparkConf = catalogInfo.foldLeft(sparkConfTmp)((conf, info) =>
      conf
        .set(s"spark.sql.catalog.${info.name}", "org.apache.iceberg.spark.SparkCatalog")
        .set(s"spark.sql.catalog.${info.name}.warehouse", "%s".format(info.path))
        .set(s"spark.sql.catalog.${info.name}.type", "hadoop")
    )

    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def deleteResDirectory(resPath: String)(implicit ss: SparkSession): Boolean = {
    FileSystem.get(new URI(resPath), ss.sparkContext.hadoopConfiguration).delete(new Path(resPath), true)
  }

  def deleteDirectory(resPath: String): Unit = new Directory(new File(resPath)).deleteRecursively()

}
