package it.jobtech.graphenj.core.models

import com.amazon.deequ.checks.{ CheckLevel, CheckStatus }
import com.amazon.deequ.constraints.ConstraintStatus

case class DeequReport(
  check: String,
  check_level: CheckLevel.Value,
  check_status: CheckStatus.Value,
  constraint: String,
  constraint_status: ConstraintStatus.Value,
  constraint_message: Option[String]
)
