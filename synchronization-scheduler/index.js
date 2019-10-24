/*
  Read with caution: really dumb scheduler!!! This need to be completely rewritten for anything else than a POC

  This scheduler is only iterating through the whole database and sending sync requests.
  Normally we should set an iterable range to distribute the load on multiple instances of the scheduler
  Right we don't have a protocol to avoid doing unecessaries synchronization (if a synchronization takes more time than expected
  we will still spam the synchronizer)

  P.S. This service is also a huge infinite loop

*/

const kafka = require('kafka-node');
const Producer = kafka.Producer;
const MongoClient = require('mongodb').MongoClient;

// CONFIG
const mongoConfig = {url: 'mongodb://localhost:27017', dbName: 'sakamin'} // TODO Put config out
const kafkaConfig = {kafkaHost: 'localhost:9092'}; // TODO Put config out
// !CONFIG

const mongoClient = new MongoClient(mongoConfig.url, {useUnifiedTopology: true});

const client = new kafka.KafkaClient(kafkaConfig);

const MSG_SYNC_TYPE = 'SYNC'

function iterate_registry(wallets) {
    const producer = new Producer(client);
    const payloads = []
    return wallets.find().forEach(function (wallet) {
        const message = {
            type: MSG_SYNC_TYPE,
            address: wallet.key,
            state: wallet.state
        }
        const payload = {
            topic: wallet.protocol,
            messages: JSON.stringify(message)
        };
        payloads.push(payload)
    }).then(function () {
        producer.send(payloads, function (err) {
            if (err)
                console.error(err)
            else
                console.log("Sent " + payloads.length + " synchronization requests.")
        })
    })
}

function loop_iterations() {
    const db = mongoClient.db(mongoConfig.dbName)
    const wallets = db.collection("wallets")
    iterate_registry(wallets).then(function() {
        setTimeout(loop_iterations, 15000)
    })
}

mongoClient.connect()
    .then(function () {
        loop_iterations()
    })
    .catch(function (err) {
        console.error(err)
        return mongoClient.close();
    })
