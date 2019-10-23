package com.ledger.sakamin.wallet_registrar

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._

object Main extends IOApp {
  def run(args: List[String]) =
    Wallet_registrarServer.stream[IO].compile.drain.as(ExitCode.Success)
}