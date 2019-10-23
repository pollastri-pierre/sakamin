var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    cosmos_consumer = new Consumer(
        client,
        [
            { topic: 'tx-stream'}
        ],
        {
            autoCommit: true
        }
    );

const MSG_UPDATE_TX_WALLET_TYPE = 'UPDATE_TX_WALLET';
const MongoClient = require('mongodb').MongoClient;

cosmos_consumer.on('message', function (message) {
    try {
        jsonMsg = JSON.parse(message.value);
        // {"type":"UPDATE_TX_WALLET","address":"cosmos1sd4tl9aljmmezzudugs7zlaya7pg2895tyn79r","transactions":
        if (jsonMsg.type.localeCompare(MSG_UPDATE_TX_WALLET_TYPE) === 0) {
            let address = jsonMsg.address;
            let transactions = jsonMsg.transactions;
            console.log('[Cosmos][Sakamin-db][Update TX] for', address);

            MongoClient.connect('mongodb://localhost:27017', function (err, client) {
                if (err) throw err;
                const db = client.db('sakamin');
                db.collection('Transactions').updateOne(
                    {address},
                    { $push: { transactions: {$each:transactions} }},
                    // { $set: { transactions: { $concatArrays: [ "$transactions", transactions ] } } },
                    { upsert: true }
                );
            });
        }
    } catch(error){
        console.error('wrong json for message', message.value);
    }
});