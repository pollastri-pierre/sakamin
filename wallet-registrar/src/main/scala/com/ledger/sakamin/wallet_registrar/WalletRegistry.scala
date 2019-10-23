package com.ledger.sakamin.wallet_registrar

import cats.Applicative
import cats.implicits._
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe._
import com.ledger.sakamin.wallet_registrar.models.Wallet
import com.ledger.sakamin.wallet_registrar.models.Protocols

trait WalletRegistry[F[_]] {
  def protocols: F[Protocols]
  def register(wallet: Wallet): F[Wallet]
}

object WalletRegistry {
  implicit def apply[F[_]](implicit ev: WalletRegistry[F]): WalletRegistry[F] = ev

  def impl[F[_]: Applicative]: WalletRegistry[F] = new WalletRegistry[F] {
    private val producer: WalletProducer = new WalletProducer(null)

    def protocols: F[Protocols] = Protocols(List("cosmos", "stellar")).pure[F] // TODO better way to deal with protocol type

    def register(w: Wallet): F[Wallet] = {
      producer.produce(w)
      w.pure[F]
    }
  }
}
