const amqp = require('amqplib/callback_api');
const mysql = require('mysql');
const bodyParser = require('body-parser')
const express = require('express');
const User = require('./User')
const app = express();
const ROUTING_KEYS = require('./RoutingKeys');

const EXCHANGE_NAME = "User"

var connection = mysql.createConnection({
    host: 'localhost',
    user: 'tristan',
    password: 'password',
});

var readStorageConnection = mysql.createConnection({
    host: 'localhost',
    user: 'tristan',
    password: 'password',
});

function initialisation(callback) {

    connection.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");

        createAndUseDatabase(() => {
            createTable(callback);
        })
    });

    readStorageConnection.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");

        createAndUseStorageDatabase(() => {
            createSorageTable();
        })
    });

    amqp.connect('amqp://localhost', function (err, connection) {
        if (err) throw err;
        console.log("Connected to RabbitMQ")
        subscribeUser(connection);
    });

    app.use(express.urlencoded({ extended: true }));
    app.use(express.json());
    configureRoutes();
    app.listen(3000);
}

function configureRoutes() {
    app.post("/user/create", (req, res, next) => {
        console.log(req.body)
        User.create(req.body);
        res.json({});
    })
}

function subscribeUser(connection) {
    connection.createChannel(function (err, channel) {
        if (err)
            throw err;

        channel.assertExchange(EXCHANGE_NAME, 'direct', {
            durable: false
        });

        channel.assertQueue('',
            {
                exclusive: true
            },
            function (err, q) {
                if (err)
                    throw err;

                Object.keys(ROUTING_KEYS).forEach(key => {
                    channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEYS[key]);
                    console.log("Queue binded to routing key : %s", ROUTING_KEYS[key])
                })

                channel.consume(q.queue, function (msg) {
                    console.log(" [x] %s: '%s'", msg.fields.routingKey, msg.content.toString());
                    let dataReceived = JSON.parse(msg.content);

                    addToEventStore({
                        internalId: dataReceived.option1,
                        other: dataReceived.option2,
                        operation: msg.fields.routingKey
                    })
                }, {
                    noAck: true
                });
            });
    });
}

function createAndUseDatabase(callback) {
    connection.query("CREATE DATABASE IF NOT EXISTS EventStoreDB", function (err, result) {
        if (err) throw err;
        console.log("Database EventStoreDB created");
        connection.query("USE EventStoreDB", (err, result) => {
            if (err) throw err;
            console.log("Using EventStoreDB")
            if (callback !== undefined)
                callback();
        });
    });
}

function createAndUseStorageDatabase(callback) {
    readStorageConnection.query("CREATE DATABASE IF NOT EXISTS StorageDB", function (err, result) {
        if (err) throw err;
        console.log("Database StorageDB created");
        readStorageConnection.query("USE StorageDB", (err, result) => {
            if (err) throw err;
            console.log("Using StorageDB")
            if (callback !== undefined)
                callback();
        });
    });
}

function createTable(callback) {
    connection.query(`CREATE TABLE IF NOT EXISTS EventStore (
    uid INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aid INT(6) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    internalId INT(6) NOT NULL,
    other VARCHAR(30) NOT NULL,
    operation VARCHAR(30) NOT NULL
    );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table EventStore created');
            if (callback != undefined)
                callback();
        }
    );
}

function createSorageTable(callback) {
    readStorageConnection.query(`CREATE TABLE IF NOT EXISTS userStorage (
        clienId INT(6) NOT NULL,
        name VARCHAR(30) NOT NULL
        );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table userStorage created');
            if (callback != undefined)
                callback();
        }
    );
    readStorageConnection.query(`CREATE TABLE IF NOT EXISTS ticketStorage (
        clientId INT(6) NOT NULL,
        ticketId INT(6) NOT NULL
        );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table tiketStorage created');
            if (callback != undefined)
                callback();
        }
    );
}

function isUser(name, callback){
    readStorageConnection.query(`SELECT EXISTS(
        SELECT *
        FROM userstorage
        WHERE name = '${name}') as isuser`,
        function (error, result, fields) {
            if (error) throw error;
            console.log(result[0].isuser)
            return callback (result[0].isuser);
        });
}

function addUser(id, name){
    readStorageConnection.query(`INSERT INTO userStorage SELECT '${id}', '${name}'`,
            function (error, results, fields) {
                if (error) throw error;
            });
}

function removeUser(name){
    readStorageConnection.query(`DELETE FROM userstorage WHERE name = '${name}'`,
    function (error, results, fields) {
        if (error) throw error;
    });
}

function addTicket(id, ticket){
    readStorageConnection.query(`INSERT INTO ticketStorage VALUES ( '${id}', '${ticket}')`,
            function (error, results, fields) {
                if (error) throw error;
            });
}

function removeTicket(id, ticket){
    readStorageConnection.query(`DELETE FROM ticketStorage WHERE clientId = '${id}' AND ticketId = '${ticket}'`,
    function (error, results, fields) {
        if (error) throw error;
    });
}

function addToEventStore(data) {
    function addToDatabase(aid, internalId, other, operation) {
        connection.query(
        `INSERT INTO EventStore
        (aid, internalId, other, operation)
        SELECT ${aid}, ${internalId}, '${other}', '${operation}'`,
            function (err, results, fields) {
                if (err)
                    throw err;
            });
    }

    if (data.operation == ROUTING_KEYS.UserGenerated) {
        isUser(data.other, function(result){
            console.log(result);
            if (result == 0){
                var maxUID = 0;
            connection.query('SELECT MAX(uid) as maxid FROM eventstore', function (err, result, fields) {
                if (err) throw err;
                maxUID = result[0].maxid;
                addToDatabase(1, maxUID + 1, data.other, data.operation);
                addUser(maxUID + 1, data.other);
          });
            }
        });
        
    } else {
        connection.query(`SELECT max(aid) from EventStore`,
            function (error, results, fields) {
                if (error) throw error;
                let aid = results[0]["max(aid)"];
                if (data.operation == ROUTING_KEYS.UserRemoved) {
                    connection.query("SELECT internalId from eventstore WHERE operation = 'UserGenerated' AND other = '" + data.other + "'", function (err, result, fields) {
                        if (err) throw err;
                        console.log(result[0].internalId);
                        addToDatabase(aid + 1, result[0].internalId, data.other, data.operation);
                        removeUser(data.other, result[0].internalId);
                      });
                }
                else{
                    addToDatabase(aid + 1, data.internalId, data.other, data.operation);

                    if (data.operation == ROUTING_KEYS.TicketAddedToCart){
                        var msg = "ticket_reserved," + data.internalId + "," + data.other;
                        sendToTicket(msg, function(result){
                            if (result == 'available'){
                                addTicket(data.internalId, data.other);
                            }
                            else {console.log("Ticket unavailaible");}
                        })
                    }
                    if (data.operation == ROUTING_KEYS.TicketRemovedFromCart){
                        var msg = "ticket_unreserved,"  + data.internalId + "," + data.other;
                        sendToTicket(msg);
                        removeTicket(data.internalId, data.other);
                    }
                }
            }
        );
    }
}

function sendToTicket (msg, callback){
    amqp.connect('amqp://localhost', function(error0, connection2) {
        connection2.createChannel(function(error1, channel) {
            if (error1) {
                throw error1;
              }

            var queue = 'rpc_queue';

            channel.assertQueue('', {
                exclusive: true
              }, function(error2, q) {
                if (error2) {
                  throw error2;
                }

                channel.consume(queue, function(msg) {
                    console.log("HELLO");
                    console.log(msg.content.toString()); 
                    return callback(msg.content.toString());           
                }, {
                    noAck: true
                });

                channel.sendToQueue(queue, Buffer.from(msg));
                console.log(" [x] Sent %s", msg);
            });
        });
    });
}

initialisation(() => {
    /*addToEventStore({
        operation: ROUTING_KEYS.TicketRemovedFromCart,
        internalId: 33,
        other: 45
    })*/
    sendToTicket("ticket");
});