/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

// We need 1 output queue
var outputQueue = parameters.require("output-queue");

// Create the Output to send Orderhead messages
stream.create().output(outputQueue).queue();

// Static data for the orderhead messages
var accounts = [1513, 271, 3300, 8800, 17777, 55167, 99220, 1513, 8800, 1900, 3344, 19220];
var orderheadids = [1011, 998, 1567, 3318, 90001, 44526, 33900, 1444, 9344, 1788, 9900, 14144];
var npositions = [3, 5, 1, 5, 6, 1, 1, 4, 8, 7, 3, 5];
var idx = 0;

// Create a timer that sends an orderhead map message every 5 secs
stream.create().timer("ticker").interval().seconds(5).onTimer(function (timer) {
    if (idx < accounts.length) {
        stream.output(outputQueue).send(stream.create().message().mapMessage().persistent()
            .property("ACCOUNTNO").set(accounts[idx])
            .property("ORDERHEADID").set(orderheadids[idx])
            .property("NPOSITIONS").set(npositions[idx])
            .body().set("ACCOUNTNO", accounts[idx])
            .set("ORDERHEADID", orderheadids[idx])
            .set("NPOSITIONS", npositions[idx]).message());
        idx++;

    } else {
        stream.create().timer("restarter").next().beginOfHour().onTimer(function (timer) {
            idx = 0;
        }).start();
    }
});
