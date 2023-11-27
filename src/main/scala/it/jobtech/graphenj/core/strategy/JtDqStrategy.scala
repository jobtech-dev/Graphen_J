package it.jobtech.graphenj.core.strategy

import com.amazon.deequ.checks.Check

trait JtDqStrategy {

  def checkQuality: Seq[Check]

}
