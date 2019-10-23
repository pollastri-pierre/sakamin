package com.ledger.sakamin.wallet_registrar

import cats.effect.Sync
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import com.ledger.sakamin.wallet_registrar.models.Wallet

object Wallet_registrarRoutes {

  def routes[F[_]: Sync](W: WalletRegistry[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F]{}
    import dsl._
    HttpRoutes.of[F] {
      case GET -> Root / "protocols" =>
        for {protocols <- W.protocols; resp <- Ok(protocols)} yield resp
      case POST -> Root / "wallets" / protocol / address =>
        for {
          wallet <- W.register(Wallet(protocol, address))
          resp <- Ok(wallet)
        } yield resp
    }
  }

}
