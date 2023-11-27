# How to define a strategy #

The strategy is that class that takes care of transforming one set of datasets into another.

Graphen_J offers the implementation of the `JtIdentityStrategy` strategy which returns the input data set without making any
changes, useful in case you simply want to move data without transforming it.

## Implement a Custom strategy

To implement a custom strategy we must necessarily extend the trait `JtEtlStrategy` and implement the `transform` method
which, given a Spark DataFrame map as input, returns an `Either[StrategyError, DataFrame]`.

The DataFrame map contains as keys all the ids indicated during the declaration of the data sources.

### Example

```scala
class PeopleStrategy extends JtEtlStrategy {

  private val PEOPLE1 = "people1"
  private val PEOPLE2 = "people2"

  override def transform(dfMap: Map[String, DataFrame]): Either[JtError.StrategyError, DataFrame] = {
    Try(dfMap(PEOPLE1).union(dfMap(PEOPLE2))) match {
      case Failure(exception) => Left(StrategyError(exception))
      case Success(df)        => Right(df)
    }
  }
}
```
