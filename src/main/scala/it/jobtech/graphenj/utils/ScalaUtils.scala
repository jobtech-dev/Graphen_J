package it.jobtech.graphenj.utils

object ScalaUtils {

  def using[A <: AutoCloseable, B](resource: A)(block: A => B): B =
    try block(resource)
    finally resource.close()

}
