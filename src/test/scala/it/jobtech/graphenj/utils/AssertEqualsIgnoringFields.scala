package it.jobtech.graphenj.utils

import org.scalatest
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object AssertEqualsIgnoringFields {

  def assertEqualsIgnoringFields[T](obj1: T, obj2: T, excludeFields: Seq[String])(implicit
    classTag: ClassTag[T],
    ttag: TypeTag[T]
  ): scalatest.Assertion = {
    val obj1Fields = extractCaseClassFields(obj1, excludeFields)
    val obj2Fields = extractCaseClassFields(obj2, excludeFields)
    obj1Fields shouldBe obj2Fields
  }

  def assertEqualsCollectionIgnoringFields[T](collection1: Seq[T], collection2: Seq[T], excludeFields: Seq[String])(
    implicit
    classTag: ClassTag[T],
    ttag: TypeTag[T]
  ): Unit = {
    collection1.length shouldBe collection2.length
    collection1.zip(collection2).foreach(pair => assertEqualsIgnoringFields(pair._1, pair._2, excludeFields))
  }

  private def extractCaseClassFields[T](obj: T, excludeFields: Seq[String])(implicit
    classTag: ClassTag[T],
    ttag: TypeTag[T]
  ) = {
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        m.asTerm.accessed.asTerm
    }.toList
      .map(mirror.reflect(obj).reflectField)
      .filterNot(x => excludeFields.contains(x.symbol.name.toString.trim))
      .map(x => (x.symbol.name, x.get))
      .toMap
  }

}
