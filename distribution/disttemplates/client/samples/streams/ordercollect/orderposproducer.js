/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

// We need 1 output queue
var outputQueue = parameters.require("output-queue");

// Create the Output to send Orderpos messages
stream.create().output(outputQueue).queue();

// Static orderpos data
var orderpos = [[14144, 4711, 2],
    [14144, 3318, 3],
    [14144, 1715, 1],
    [9900, 1522, 1],
    [9900, 3318, 2],
    [9900, 4711, 1],
    [1788, 4355, 1],
    [1788, 6722, 1],
    [1011, 1677, 3],
    [998, 1544, 1],
    [998, 4711, 1],
    [1788, 2011, 4],
    [1788, 1988, 1],
    [1788, 3789, 1],
    [9344, 1988, 1],
    [9344, 2011, 1],
    [9344, 9211, 1],
    [998, 9211, 1],
    [998, 3318, 1],
    [9344, 1031, 2],
    [9344, 1544, 1],
    [9344, 1677, 1],
    [1444, 1988, 1],
    [1444, 6722, 4],
    [90001, 1677, 3],
    [14144, 2011, 2],
    [14144, 1988, 5],
    [90001, 3318, 2],
    [90001, 2011, 5],
    [1788, 1566, 3],
    [1788, 9913, 1],
    [90001, 1677, 1],
    [90001, 4355, 2],
    [3318, 1988, 2],
    [3318, 9913, 2],
    [9344, 3318, 1],
    [9344, 4417, 4],
    [33900, 6722, 1],
    [44526, 4711, 2],
    [90001, 9211, 2],
    [3318, 4355, 2],
    [1444, 3318, 2],
    [1444, 4355, 3],
    [3318, 1544, 2],
    [3318, 1677, 2],
    [1567, 4711, 1],
    [998, 2011, 10],
    [1011, 1031, 2],
    [1011, 4444, 11]];
var idx = 0;

// Create a timer that sends one orderpos per second
stream.create().timer("ticker").interval().seconds(5).onTimer(function (timer) {
    if (idx < orderpos.length) {
        stream.output(outputQueue).send(stream.create().message().mapMessage().persistent()
            .property("ORDERHEADID").set(orderpos[idx][0])
            .property("ITEMNO").set(orderpos[idx][1])
            .property("QTY").set(orderpos[idx][2])
            .body().set("ORDERHEADID", orderpos[idx][0])
            .set("ITEMNO", orderpos[idx][1])
            .set("QTY", orderpos[idx][2]).message());
        idx++;
    }
    else {
        stream.create().timer("restarter").next().beginOfHour().onTimer(function (timer) {
            idx = 0;
        }).start();
    }
});
