package it.jobtech.graphenj.configuration.model

sealed trait Provider

object Iceberg extends Provider {
  private val ICEBERG           = "iceberg"
  override def toString: String = ICEBERG
}
