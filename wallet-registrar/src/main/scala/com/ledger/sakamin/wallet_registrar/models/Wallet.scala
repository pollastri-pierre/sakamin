package com.ledger.sakamin.wallet_registrar.models

import cats.Applicative
import cats.implicits._
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe._

case class Wallet(protocol: String, key: String)

object Wallet {
    implicit val encoder: Encoder[Wallet] = new Encoder[Wallet] {
      final def apply(w: Wallet): Json = Json.obj(
        ("protocol", Json.fromString(w.protocol)),
        ("key", Json.fromString(w.key))
      )
    }
    implicit def walletEntityEncoder[F[_]: Applicative]: EntityEncoder[F, Wallet] =
      jsonEncoderOf[F, Wallet]
}
