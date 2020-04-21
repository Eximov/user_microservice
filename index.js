#!/usr/bin/env node

var amqp = require('amqplib/callback_api');
const EXCHANGE_NAME = "User"

amqp.connect('amqp://localhost', function (error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function (error1, channel) {
        if (error1) {
            throw error1;
        }

        channel.assertExchange(EXCHANGE_NAME, 'direct', {
            durable: false
        });
        channel.publish(EXCHANGE_NAME, "TicketSold", Buffer.from(JSON.stringify({ message: "Coucou" })));
        console.log(" [x] Sent ");
    });

    setTimeout(function () {
        connection.close();
        process.exit(0)
    }, 500);
});