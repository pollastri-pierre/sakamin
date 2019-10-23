package com.ledger.sakamin.wallet_registrar

import java.util.Properties
import org.apache.kafka.clients.producer._
import com.ledger.sakamin.wallet_registrar.models.Wallet

class WalletProducer(config: Config) {
  val WALLET_TOPIC: String = "register-wallet"

  //private val producer: new KafkaProducer()

  def produce(wallet: Wallet): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](WALLET_TOPIC, null, Wallet.encoder(wallet).toString())
    producer.send(record)
    producer.close()
  }

}
