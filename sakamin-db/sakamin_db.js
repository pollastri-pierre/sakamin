var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    cosmos_consumer = new Consumer(
        client,
        [
            { topic: 'tx-stream' },
            { topic: 'account-update-stream' }
        ],
        {
            autoCommit: true
        }
    );

const MSG_UPDATE_TX_WALLET_TYPE = 'UPDATE_TX_WALLET';
const MongoClient = require('mongodb').MongoClient;

const handlers = {}
handlers["tx-stream"] = function (db, jsonMsg) {
    // {"type":"UPDATE_TX_WALLET","address":"cosmos1sd4tl9aljmmezzudugs7zlaya7pg2895tyn79r","transactions":
    if (jsonMsg.type.localeCompare(MSG_UPDATE_TX_WALLET_TYPE) === 0) {
        let address = jsonMsg.address;
        let collection = db.collection('transactions')
        let transactions = jsonMsg.transactions.forEach(function (transaction) {
            console.log('[Cosmos][Sakamin-db][Update TX] for', address);
            collection.updateOne({id: transaction.id}, {$set: transaction}, {upsert: true})
        });
    }
}

handlers["account-update-stream"] = function (db, jsonMsg) {
    console.log("Update account")
    let collection = db.collection('account-updates')
    collection.insertOne(jsonMsg)
}

MongoClient.connect('mongodb://localhost:27017', function (err, client) {
    if (err) throw err;
    const db = client.db('sakamin');
    cosmos_consumer.on('message', function (message) {
        try {
            jsonMsg = JSON.parse(message.value);
            handlers[message.topic](db, jsonMsg);
        } catch(error){
            console.error(error)
            console.error('wrong json for message', message.value);
        }
    });
});
