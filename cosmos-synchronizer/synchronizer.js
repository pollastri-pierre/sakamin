var request = require('request');
var sha256 = require('js-sha256');
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    ConsumerGroup = kafka.ConsumerGroup,
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.KafkaClient();

var options = {
    kafkaHost: 'localhost:9092', // connect directly to kafka broker (instantiates a KafkaClient)
    batch: undefined, // put client batch settings if you need them
    ssl: false, // optional (defaults to false) or tls options hash
    groupId: 'cosmos-synchronizer',
    sessionTimeout: 1500000,
    autoCommit: true,
    // An array of partition assignment protocols ordered by preference.
    // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
    protocol: ['roundrobin'],
    encoding: 'utf8', // default is utf8, use 'buffer' for binary data

    // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
    // equivalent to Java client's auto.offset.reset
    fromOffset: 'latest', // default
    commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
    // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
    outOfRangeOffset: 'earliest', // default
    fetchMaxBytes: 150000000
    // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
    // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
    // onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
};

var cosmos_consumer = new ConsumerGroup(options, 'cosmos');





producer = new HighLevelProducer(client);
const MSG_SYNC_TYPE = 'SYNC'
const MSG_UPDATE_TX_WALLET_TYPE = 'UPDATE_TX_WALLET'
const MSG_UPDATE_STATE_WALLET_TYPE = 'UPDATE_STATE_WALLET'
const api_url = 'https://sgapiv2.certus.one/v1/transactions?sender=';

function to_sakamin_db(raw_tx) {
    return raw_tx.messages.map((tx, idx) => {
        message = JSON.parse(tx.data);
        let amount = 0;
        let to = '';
        let fees = 0;
        if ('amount' in message) {
            if (message.amount.length > 1) {
                amount = message.amount[0].amount
            } else {
                amount = message.amount
            }
            to = message.to_address
        } else {
            to = message.validator_address
        }

        if (raw_tx.fees.amount.length > 1) {
            fees = tx.fees.amount[0].amount
        }

        return {
            id: sha256(''+raw_tx.hash + idx + tx.type + 'cosmos'),
            type: tx.type,
            amount: amount,
            to: to,
            fees: fees,
            raw_tx: raw_tx
        }
    })
}

function flatten_array(arr) {
    return [].concat.apply([], arr);
}

function get_tx(height, transactions) {
    return transactions.filter(x => x.height >= ''+height)
}

cosmos_consumer.on('message', function (message) {
    // console.log(cosmos_consumer)
    // msg interface {"type": "SYNC", "address":"cosmos1sd4tl9aljmmezzudugs7zlaya7pg2895tyn79r", "height":2291227}
    // msg interface {"type": "SYNC", "address":"cosmos1s43l7s99hx627scn2ldd3ey50qpj9frv89zkyv"}
    try {
        jsonMsg = JSON.parse(message.value);
        if (jsonMsg.type.localeCompare(MSG_SYNC_TYPE) === 0) {
            let address = jsonMsg.address;
            let height = '';
            if (jsonMsg.state) {
                height = jsonMsg.state.height || 0
            }
            console.log('[Cosmos][Coin Synchronizer][Sync Request] for', address, height)
            request(api_url + address, function (error, response, body) {
                resp = JSON.parse(body); // Print the HTML for the Google homepage.
                // todo if height not here we should keep query
                last_txs = get_tx(height, resp.transactions)
                format_txs = last_txs.map(x => to_sakamin_db(x))
                console.log('[Cosmos][Coin Synchronizer] latest transactions obtains for', address, height)
                const update_tx_msg = {
                    type: MSG_UPDATE_TX_WALLET_TYPE,
                    address: address,
                    transactions: flatten_array(format_txs)
                }

                let message_to_public = [JSON.stringify(update_tx_msg)]
                payloads = [
                    {topic: 'tx-stream', messages: message_to_public},
                ];
                if (last_txs.length > 0) {
                    const new_height = last_txs.length > 0 ? last_txs[0].height : height
                    const update_height_msg = {
                        type: MSG_UPDATE_STATE_WALLET_TYPE,
                        address: address,
                        protocol: 'cosmos',
                        state: {
                            height: new_height
                        }
                    }
                    console.log('[Cosmos][Coin Synchronizer][Publish Latest account state]', address, new_height)
                    console.log(JSON.stringify(update_height_msg))
                    payloads.push({
                        topic: 'update-wallet',
                        messages: JSON.stringify(update_height_msg)
                    })
                }

                producer.send(payloads, function (err, data) {
                    console.log('[Cosmos][Coin Synchronizer][Publish Latest transactions] add', address, flatten_array(format_txs).length, 'transactions')
                });
            });
        }
    } catch(error){
        console.error('wrong json for message', message.value);
        console.error(error)
    }
});

cosmos_consumer.on('connect', function (message) {
    console.log('connect')
});
