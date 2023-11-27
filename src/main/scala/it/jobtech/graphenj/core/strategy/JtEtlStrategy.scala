package it.jobtech.graphenj.core.strategy

import it.jobtech.graphenj.utils.JtError.StrategyError
import org.apache.spark.sql.DataFrame

trait JtEtlStrategy {

  def transform(dfMap: Map[String, DataFrame]): Either[StrategyError, DataFrame]

}

class JtIdentityEtlStrategy extends JtEtlStrategy {

  override def transform(dfMap: Map[String, DataFrame]): Either[StrategyError, DataFrame] = {
    if (dfMap.keySet.size > 1) {
      val msg = "Impossible to apply the JtIdentityStrategy: more sources are specified, only one is required!"
      Left(StrategyError(new IllegalArgumentException(msg)))
    } else {
      Right(dfMap.values.head)
    }
  }
}
