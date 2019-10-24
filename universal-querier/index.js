const restify = require('restify');
const MongoClient = require('mongodb').MongoClient;
var errors = require('restify-errors');

// CONFIG
const mongoConfig = {url: 'mongodb://localhost:27017', dbName: 'sakamin'} // TODO Put config out
// !CONFIG

var server = restify.createServer();

const mongoClient = new MongoClient(mongoConfig.url, {useUnifiedTopology: true});

function get_transactions(owner, transactions) {
    return transactions.find({owner: owner}).toArray();
}

function get_last_account_update(owner, account_updates) {
    return account_updates.find({owner: owner}).sort({time: -1}).toArray();
}

mongoClient.connect().then(function () {
    const db = mongoClient.db(mongoConfig.dbName);
    const transactions = db.collection("transactions");
    const account_updates = db.collection("account-updates");

    server.get('/:protocol/:address/transactions', function (req, res, next) {
        const owner = req.params.protocol + ":" + req.params.address
        get_transactions(owner, transactions).then(function (txs) {
            if (txs.length > 0) {
                res.send({txs: txs});
                next();
            } else {
                next(new errors.NotFoundError("No transaction found for " + owner));
            }
        })
    });

    server.get('/:protocol/:address/account', function (req, res, next) {
        const owner = req.params.protocol + ":" + req.params.address
        get_last_account_update(owner, account_updates).then(function (account) { // TODO will fail if first sync
            if (account.length > 0) {
                res.send(account[0])
            } else {
                next(new errors.NotFoundError("No account found for " + owner))
            }
        })
    });

    server.listen(8082, function() {
        console.log('%s listening at %s', server.name, server.url);
    });
}).catch(function (err) {
    console.error(err)
})
