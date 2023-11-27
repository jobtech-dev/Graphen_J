package it.jobtech.graphenj.configuration.model

sealed trait JtJobType

object ETL extends JtJobType {
  override def toString: String = "ETL"
}

object DQ extends JtJobType {
  override def toString: String = "DQ"
}

object ETL_DQ extends JtJobType {
  override def toString: String = "ETL_DQ"
}
