const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

const mongoConfig = {url: 'mongodb://localhost:27017', dbName: 'sakamin'} // TODO Put config out
const kafkaConfig = {kafkaHost: 'localhost:9092'}; // TODO Put config out
const client = new kafka.KafkaClient();
const consumer = new Consumer(
        client,
    [ { topic: 'register-wallet' }, {topic: 'update-wallet'} ],
    { autoCommit: true, fromOffset: false } // TODO Use a manual commit mamagement otherwise potential data loss
);

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = 'myproject';

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
    msg.url = msg.protocol + ":" + msg.key;
    const db = mongo.db(mongoConfig.dbName)
    const collection = db.collection("wallets")
    collection.updateOne({url: msg.url}, {$set: msg}, {upsert: true}, function(err) {
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
