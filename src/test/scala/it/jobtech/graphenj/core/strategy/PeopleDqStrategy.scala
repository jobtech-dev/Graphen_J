package it.jobtech.graphenj.core.strategy

import com.amazon.deequ.checks.{ Check, CheckLevel }
import com.github.dwickern.macros.NameOf.nameOf
import it.jobtech.graphenj.utils.models.Person

class PeopleDqStrategy extends JtDqStrategy {
  override def checkQuality: Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "Person test dataset")
        .hasSize(_ == 4)
        .isComplete(nameOf[Person](_.firstName))
        .isComplete(nameOf[Person](_.lastName))
    )
  }
}

class PeopleDqEtlDqTestStrategy extends JtDqStrategy {
  override def checkQuality: Seq[Check] = {
    Seq(
      Check(CheckLevel.Error, "Person test dataset")
        .isComplete(nameOf[Person](_.firstName))
        .isComplete(nameOf[Person](_.lastName))
        .isUnique(nameOf[Person](_.id))
    )
  }
}
