package com.ledger.sakamin.wallet_registrar.models

import cats.Applicative
import cats.implicits._
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe._

case class Protocols(protocols: List[String])

object Protocols {
    implicit val encoder: Encoder[Protocols] = new Encoder[Protocols] {
      final def apply(p: Protocols): Json = Json.obj(
        ("protocols", Json.fromValues(p.protocols.map(Json.fromString)))
      )
    }
    implicit def walletEntityEncoder[F[_]: Applicative]: EntityEncoder[F, Protocols] =
      jsonEncoderOf[F, Protocols]
}
