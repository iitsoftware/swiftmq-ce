/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

// Check parameters
var outputQueue = parameters.require("output-queue");

// Create the Output to send the temp messages
stream.create().output(outputQueue).queue();

// Static temp data that will generate a warning and critical condition
var temps = [90, 97, 101, 80, 90, 110, 120, 200, 250, 500, 450, 320, 401, 402, 450, 800];
var idx = 0;

// Timer to send temps in a 2 secs interval
stream.create().timer("ticker").interval().seconds(2).onTimer(function (timer) {
    stream.log().info("Sending temp: " + temps[idx]);
    var msg = stream.create().message().message().nonpersistent().property("TEMP").set(temps[idx++]);
    if (idx == temps.length)
        idx = 0;
    stream.output(outputQueue).send(msg);
});
