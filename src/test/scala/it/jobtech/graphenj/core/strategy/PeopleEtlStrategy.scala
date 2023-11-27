package it.jobtech.graphenj.core.strategy

import it.jobtech.graphenj.utils.JtError
import it.jobtech.graphenj.utils.JtError.StrategyError
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }

class PeopleEtlStrategy extends JtEtlStrategy {

  private val PEOPLE1 = "people1"
  private val PEOPLE2 = "people2"

  override def transform(dfMap: Map[String, DataFrame]): Either[JtError.StrategyError, DataFrame] = {
    Try(dfMap(PEOPLE1).union(dfMap(PEOPLE2))) match {
      case Failure(exception) => Left(StrategyError(exception))
      case Success(df)        => Right(df)
    }
  }
}
