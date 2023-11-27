package it.jobtech.graphenj.configuration.model

trait JtSaveMode

object ErrorIfExists extends JtSaveMode {
  override def toString: String = "ErrorIfExists"
}

object Overwrite extends JtSaveMode {
  override def toString: String = "Overwrite"
}

object Append extends JtSaveMode {
  override def toString: String = "Append"
}

object AppendOnlyNewRows extends JtSaveMode {
  override def toString: String = "AppendOnlyNewRows"
}

object MergeIntoById extends JtSaveMode {
  override def toString: String = "MergeIntoById"
}

object Custom extends JtSaveMode {
  override def toString: String = "Custom"
}
