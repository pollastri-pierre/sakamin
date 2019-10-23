var request = require('request');
var sha256 = require('js-sha256');
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    HighLevelProducer = kafka.HighLevelProducer,
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
    // msg interface {"type": "SYNC", "address":"cosmos1sd4tl9aljmmezzudugs7zlaya7pg2895tyn79r", "height":2291227}
    // msg interface {"type": "SYNC", "address":"cosmos1s43l7s99hx627scn2ldd3ey50qpj9frv89zkyv"}
    try {
        jsonMsg = JSON.parse(message.value);
        if (jsonMsg.type.localeCompare(MSG_SYNC_TYPE) === 0) {
            let address = jsonMsg.address;
            let height = '';
            if ('height' in jsonMsg) {
                height = jsonMsg.height;
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
                if (last_txs.length > 0) {
                    const new_height = last_txs.length > 0 ? last_txs[0].height : height
                    const update_height_msg = {
                        type: MSG_UPDATE_STATE_WALLET_TYPE,
                        address: address,
                        height: new_height
                    }
                    console.log('[Cosmos][Coin Synchronizer][Publish Latest account state]', address, new_height)
                    message_to_public.push(JSON.stringify(update_height_msg))
                }
                payloads = [
                    {topic: 'cosmos', messages: message_to_public},
                ];
                producer.send(payloads, function (err, data) {
                    console.log('[Cosmos][Coin Synchronizer][Publish Latest transactions] add', address, flatten_array(format_txs).length, 'transactions')
                });
            });
        }
    } catch(error){
        console.error('wrong json for message', message.value);
    }
});