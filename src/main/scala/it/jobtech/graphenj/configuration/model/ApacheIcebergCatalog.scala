package it.jobtech.graphenj.configuration.model

sealed trait ApacheIcebergCatalogType

object Hadoop extends ApacheIcebergCatalogType {
  val HADOOP                    = "hadoop"
  override def toString: String = HADOOP
}

object Glue extends ApacheIcebergCatalogType {
  val GLUE                      = "glue"
  override def toString: String = GLUE
}

sealed trait ApacheIcebergCatalog extends Product with Serializable {
  def catalogType: ApacheIcebergCatalogType
}

object ApacheIcebergCatalog {

  val ICEBERG_SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog"
  val ICEBERG_CATALOG       = "org.apache.iceberg.spark.SparkSessionCatalog"

  final case class HadoopApacheIcebergCatalog(catalogName: String, warehouse: String, s3aEndpoint: Option[String])
      extends ApacheIcebergCatalog {
    override def catalogType: ApacheIcebergCatalogType = Hadoop
  }

  final case class GlueApacheIcebergCatalog(catalogName: String, warehouse: String) extends ApacheIcebergCatalog {
    override def catalogType: ApacheIcebergCatalogType = Glue
    val catalogImpl                                    = "org.apache.iceberg.aws.glue.GlueCatalog"
  }
}
