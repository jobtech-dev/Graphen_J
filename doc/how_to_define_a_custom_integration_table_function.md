# How to define a custom integration table function #

In the [Data destination configurations](destination_configuration.md) it was introduced that a custom function can be
implemented to integrate data into an existing SparkTable. In this section we see how to implement one.

## Implement a Custom IntegrationTableFunction

To implement a custom function we must necessarily extend the abstract class `IntegrationTableFunction` and implement
the `function` method.

The function method must return a function that takes as input a Spark DataFrame and a DestinationDetail.SparkTable and
must return an `Either[JtError.WriteError, Unit]`.

### Example

```scala
class PeopleIntegrationTableFunction(implicit ss: SparkSession) extends IntegrationTableFunction {

  override def function(): (DataFrame, DestinationDetail.SparkTable) => Either[JtError.WriteError, Unit] = {
    (df: DataFrame, dest: SparkTable) =>
      {
        df.createOrReplaceTempView("my_view")
        Try(ss.sql(sqlText = s"INSERT INTO my_catalog.db.people (SELECT * FROM my_view)")) match {
          case Failure(exception) => Left(WriteError(dest, exception))
          case Success(_)         => Right(())
        }
      }
  }
}
```

