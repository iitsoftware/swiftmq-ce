/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 *
 * This example implements the EI-Pattern "Aggregator". It receives OrderHead
 * and OrderPos messages from different queues and generates a XML messages
 * of the full order once completely received.
 */

// 3 queues are required: orderhead, orderpos, and the output
var orderHeadQueue = parameters.require("orderhead-queue");
var orderPosQueue = parameters.require("orderpos-queue");
var outputQueue = parameters.require("output-queue");


// Create the memories: orderhead, orderpos
stream.create().memory("orderhead").queue("stream_ordercollector_memory_orderhead").createIndex("ORDERHEADID");
stream.create().memory("orderpos").queue("stream_ordercollector_memory_orderpos").createIndex("ORDERHEADID");
stream.memory("orderhead").clear();
stream.memory("orderpos").clear();

// Create the inputs
stream.create().input(orderHeadQueue).queue().onInput(function (input) {
    // Add message to orderhead memory
    stream.memory("orderhead").add(input.current());
});

stream.create().input(orderPosQueue).queue().onInput(function (input) {
    // We need ITEMNO and QTY as a property
    input.current().property("ITEMNO").set(input.current().body().get("ITEMNO").toInteger());
    input.current().property("QTY").set(input.current().body().get("QTY").toInteger());

    // Add message to orderpos memory
    stream.memory("orderpos").add(input.current());
});

// Purge output queue every hour
stream.create().timer("purger").interval().hours(1).onTimer(function (timer) {
  stream.cli()
      .exceptionOff()
      .execute("cc sys$queuemanager/usage")
      .execute("remove " + outputQueue + " *");
});

// Create an Output to send the collected Orders
stream.create().output(outputQueue).queue();

// Create the onMessage callback
stream.onMessage(function () {

    // Get the orderhead id from the current message (may be a orderhead or orderpos message)
    var orderHeadId = stream.current().property("ORDERHEADID").value().toInteger();

    // Get all orderheads we currently have for this orderhead id
    var orderHeadMem = stream.memory("orderhead").index("ORDERHEADID").get(orderHeadId);

    // Check if we have one orderhead for this orderhead
    if (orderHeadMem.size() == 1) {

        // Join it with the orderpos memory
        var orderPosMem = orderHeadMem.join(stream.memory("orderpos"), "ORDERHEADID");

        // Check if we have all orderpositions mentioned in the orderhead
        if (orderHeadMem.first().property("NPOSITIONS").value().toInteger() == orderPosMem.size()) {

            // Generate XML message with the completed order
            var orderHead = orderHeadMem.first();
            var msg = "<order orderheadid=\"" + orderHead.body().get("ORDERHEADID").toStringValue() + "\"" +
                " accountno=\"" + orderHead.body().get("ACCOUNTNO").toStringValue() + "\">\n";

            orderPosMem.forEach(function (message) {
                msg += "  <position itemno=\"" + message.property("ITEMNO").value().toString() + "\"" +
                    " quantity=\"" + message.property("QTY").value().toString() + "\"/>\n";

            });

            msg += "</order>";

            // Log it into the stream's log file
            stream.log().info(msg);

            // Send it to the output queue as persistent text message
            stream.output(outputQueue).send(stream.create().message().textMessage().persistent().body(msg));

            // Remove the messages with this orderhead id from orderhead and orderpos memories
            stream.memory("orderhead").index("ORDERHEADID").remove(orderHeadId);
            stream.memory("orderpos").index("ORDERHEADID").remove(orderHeadId);
        }
    }
});
