var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    cosmos_consumer = new Consumer(
        client,
        [
            { topic: 'cosmos'}
        ],
        {
            autoCommit: true
        }
    );

const MSG_UPDATE_TX_WALLET_TYPE = 'UPDATE_TX_WALLET';


cosmos_consumer.on('message', function (message) {
    try {
        jsonMsg = JSON.parse(message.value);
        // {"type":"UPDATE_TX_WALLET","address":"cosmos1sd4tl9aljmmezzudugs7zlaya7pg2895tyn79r","transactions":
        if (jsonMsg.type.localeCompare(MSG_UPDATE_TX_WALLET_TYPE) === 0) {
            let address = jsonMsg.address;
            let transaction = jsonMsg.transactions;
            console.log('[Cosmos][Sakamin-db][Update TX] for', address)
        }
    } catch(error){
        console.error('wrong json for message', message.value);
    }
});