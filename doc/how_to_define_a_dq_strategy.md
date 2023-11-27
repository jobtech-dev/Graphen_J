# How to define a data quality strategy #

The data quality strategy is the class that is meant to provide the check to be done on the data.

## Implement a data quality strategy

To define your data quality strategy you must first define a class that extends the `JtDqStrategy` trait. This class
will have to implement a `def checkQuality: Seq[Check]` method to define the checks on the data.

#### Example

```scala
class DqPersonStrategy extends JtDqStrategy {
  override def checkQuality: Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "Person test dataset")
        .hasSize(_ == 4)
        .isComplete("name")
        .isComplete("surname")
    )
  }
}
```