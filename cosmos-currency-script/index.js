const restify = require('restify');

// CONFIG
const uqConfig = {url: 'http://localhost:8082'} // TODO Put config out
// !CONFIG

var server = restify.createServer();

function prepare_tx(req, res, next) {

}

function broadcast_tx(req, res, next) {

}

function validate_addresses(req, res, next) {

}

server.post("/transaction/:address/prepare", prepare_tx)
server.post("/transaction/broadcast", broadcast_tx)
server.post("/addresses/validate", validate_addresses)

server.listen(8083, function() {
    console.log('%s listening at %s', server.name, server.url);
});
