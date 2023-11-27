package it.jobtech.graphenj.models

import io.circe.generic.semiauto._
import io.circe.{ Decoder, Encoder }

case class JtBookmark[T](entry: String, lastRead: T, timeMillis: Long)

object JtBookmark {
  implicit def encoder[T](implicit encoderT: Encoder[T]): Encoder[JtBookmark[T]] = deriveEncoder[JtBookmark[T]]
  implicit def decoder[T: Decoder]: Decoder[JtBookmark[T]]                       = deriveDecoder[JtBookmark[T]]
}
