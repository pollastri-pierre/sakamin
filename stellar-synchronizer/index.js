var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    ConsumerGroup = kafka.ConsumerGroup,
    HighLevelProducer = kafka.HighLevelProducer,
    client = new kafka.KafkaClient();
var sha256 = require('js-sha256');

var options = {
    kafkaHost: 'localhost:9092', // connect directly to kafka broker (instantiates a KafkaClient)
    groupId: 'stellar-synchronizer',
    autoCommit: true,
};

var consumer = new ConsumerGroup(options, 'stellar');
var StellarSdk = require('stellar-sdk');
var server = new StellarSdk.Server('https://horizon.stellar.org');

const producer = new HighLevelProducer(client);
const MSG_SYNC_TYPE = 'SYNC'
const MSG_UPDATE_TX_WALLET_TYPE = 'UPDATE_TX_WALLET'
const MSG_UPDATE_STATE_WALLET_TYPE = 'UPDATE_STATE_WALLET'

function consume_all_transactions(accountId, page, ops) {
    if (page.records.length > 0) {
        page.records.forEach(function (r) {
            const type = r.from == accountId ? "send" : "receive";
            const tx = {
                id: sha256(''+ r.transaction_hash + 0 + type + 'stellar:' + accountId),
                type: type,
                from: r.from,
                to: r.to,
                amount: r.amount,
                fees: 0, // TODO No!
                time: r.created_at,
                owner: "stellar:" + accountId,
                raw_tx: r
            }
            delete r.self
            delete r.transaction
            delete r.effects
            delete r.succeeds
            delete r.precedes
            delete r._links
            ops.push(tx)
        })
        return page.next().then(function (p) { return consume_all_transactions(accountId, p, ops) });
    } else {
        return ops
    }
}

function get_transactions(accountId, state) {
    let request = server.operations()
        .forAccount(accountId)
    if (state && state.cursor) {
        request = request.cursor(state.cursor)
    }
    return request.call().then(function (p) { return consume_all_transactions(accountId, p, []) })
}

function get_account(accountId) {
    return server.accounts().accountId(accountId).call().then(function (acc) {
        return {
            address: accountId,
            owner: "stellar:" + accountId,
            sequence: acc.sequence,
            balance: acc.balances[0].balance
        };
    })
}

consumer.on('message', function (message) {
    const msg = JSON.parse(message.value)
    get_transactions(msg.address, msg.state).then(function (txs) {
        const update_tx_msg = {
            type: MSG_UPDATE_TX_WALLET_TYPE,
            address: msg.address,
            transactions: txs
        }
        let message_to_public = [JSON.stringify(update_tx_msg)]
        payloads = [
            {topic: 'tx-stream', messages: message_to_public},
        ]
        producer.send(payloads, function (err, data) {
            console.log('[Stellar][Coin Synchronizer][Publish Latest transactions] add', msg.address, txs.length, 'transactions')
        });
        if (txs.length > 0) {
            const cursor = txs[txs.length - 1].raw_tx.paging_token
            const update_height_msg = {
                type: MSG_UPDATE_STATE_WALLET_TYPE,
                address: msg.address,
                protocol: 'stellar',
                state: {
                    cursor: cursor
                }
            }
            console.log('[Cosmos][Coin Synchronizer][Publish Latest account state]', msg.address, cursor)
            console.log(JSON.stringify(update_height_msg))
            payloads.push({
                topic: 'update-wallet',
                messages: JSON.stringify(update_height_msg)
            })
        }
    }).catch(function (err) {
        console.error(err)
    })
    get_account(msg.address).then(function (acc) {
        producer.send([{topic: 'account-update-stream', messages: JSON.stringify(acc)}],
                      function (err, data) {
                          console.log('[Cosmos][Coin Synchronizer][Publish Account update] for', msg.address);
                      });
    })
});
