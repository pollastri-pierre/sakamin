const config = require('/etc/sakamin/config.json');
const kafkaconf = config.kafka;
const mongoconf = config.mongo;

const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

const mongoConfig = {url: mongoconf.host, dbName: 'sakamin'} // TODO Put config out
const kafkaConfig = {kafkaHost: kafkaconf.servers}; // TODO Put config out
const client = new kafka.KafkaClient();
const consumer = new Consumer(
        client,
    [ { topic: 'register-wallet' }, {topic: 'update-wallet'} ],
    { autoCommit: true, fromOffset: false } // TODO Use a manual commit mamagement otherwise potential data loss
);

// Create a new MongoClient
const mongoClient = new MongoClient(mongoConfig.url, {useUnifiedTopology: true});

const handlers = {}
handlers["register-wallet"] = function (msg, mongo) {
    msg.url = msg.protocol + ":" + msg.key;
    const db = mongo.db(mongoConfig.dbName)
    const collection = db.collection("wallets")
    collection.findOne({url: msg.url})
        .then(function (res) {
            if (!res) {
                msg.state = null
                collection.insertOne(msg, function(err) {
                    mongo.close()
                })
            }
        })
        .catch(function () {
            console.error("Fatal error system") // TODO Yeah seriously
        })
}

handlers["update-wallet"] = function (msg, mongo) {
    msg.url = msg.protocol + ":" + msg.address;
    const wallet = {
        url: msg.protocol + ":" + msg.address,
        protocol: msg.protocol,
        key: msg.address,
        state: msg.state
    }
    const db = mongo.db(mongoConfig.dbName)
    const collection = db.collection("wallets")
    collection.updateOne({url: wallet.url}, {$set: wallet}, {upsert: true}, function(err) {
        mongo.close()
    })
}

consumer.on("message", function (message) {
    console.log(message)
    const msg = JSON.parse(message.value);
    mongoClient.connect(function (err) {
        if (err)
            throw err;
        handlers[message.topic](msg, mongoClient) // TODO This will break
    })
})
