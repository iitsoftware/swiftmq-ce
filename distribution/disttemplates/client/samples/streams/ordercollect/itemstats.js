/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 *
 * This example demonstrates: WireTap, Management Input, Timer reconfiguration.
 * It creates a wiretap into the orderpos queue and generates a statistic.
 */

// Parameters
var orderPosQueue = parameters.require("orderpos-queue");
var statIntervalSec = parameters.require("statistic-interval-sec");

// Callback function for onRetire to print the statistics
function printStatistic(retired) {

    // Generate statistics in the streams's log file
    stream.log().info("Item Statistics:");

    // Get and log the statistic data (items with summarized quantity in descending order)
    retired.group("ITEMNO").sum("QTY").sort("QTY").reverse().forEach(function (message) {
        stream.log().info(message.property("ITEMNO").value().toString() + " = " + message.property("QTY").value().toString());
    });
}

// Create the itemstats memories for the item statistics
stream.create().memory("itemstats").heap().limit().time().tumbling().seconds(statIntervalSec).onRetire(printStatistic);

// Create a timer that triggers the item statistics every n secs
stream.create().timer("stats").interval().seconds(statIntervalSec).onTimer(function (timer) {
    // Reduce the memory according to the limit
    stream.memory("itemstats").checkLimit();
});


// Create a wiretap input on the orderpos queue.
stream.create().input(orderPosQueue).wiretap("w1").onInput(function (input) {
    // We need ITEMNO and QTY as a property
    input.current().property("ITEMNO").set(input.current().body().get("ITEMNO").toInteger());
    input.current().property("QTY").set(input.current().body().get("QTY").toInteger());

    stream.memory("itemstats").add(input.current());
});

// Create a Management Input that receives changes on my own statistic-interval-sec parameter
// and reconfigures the Timer
stream.create().input("sys$streams/domains/swiftmq/packages/samples/streams/" + stream.name() + "/parameters/statistic-interval-sec").management().onChange(function (input) {

    // Get the new value
    var secs = input.current().property("value").value().toInteger();

    // Reset and reconfigure the Timer with the new value
    stream.timer("stats").reset().seconds(secs).reconfigure();

    // Recreate the itemstats Memory
    stream.memory("itemstats").close();
    stream.create().memory("itemstats").heap().limit().time().tumbling().seconds(secs).onRetire(printStatistic);

    // Log it into the Logfile
    stream.log().info("Statistic interval reconfigured to " + secs + " seconds");

});
