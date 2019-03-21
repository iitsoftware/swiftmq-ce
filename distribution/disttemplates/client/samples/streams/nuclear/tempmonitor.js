/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

// Checking parameters
var inputQueue = parameters.require("input-queue");
var mailHost = parameters.optional("mail-host", "localhost");
var mailUser = parameters.require("mail-user");
var mailPassword = parameters.require("mail-password");
var mailFrom = parameters.require("mail-from");
var mailTo = parameters.require("mail-to");
var mailBcc = parameters.require("mail-bcc");

// Create 3 Memories, one for monitoring (all), one to detect warning, one to detect critical conditions
stream.create().memory("all").heap().limit().time().tumbling().seconds(10).onRetire(function (retired) {
    stream.log().info("AVERAGE TEMP LAST 10 SECS=" + retired.average("temp") +
        ", MIN=" + retired.min("temp").property("temp").value().toObject() +
        ", MAX=" + retired.max("temp").property("temp").value().toObject());
    stream.log().info(retired.values("temp"));
});
stream.create().memory("warning").heap().limit().count(2).sliding();
stream.create().memory("critical").heap().limit().count(4).sliding();

// Create a mail server to send critical mails
stream.create().mailserver(mailHost).username(mailUser).password(mailPassword).connect();

// Create the Inputs
stream.create().input(inputQueue).queue().onInput(function (input) {
    // We need a property "temp" instead of "TEMP"
    input.current().property("temp").set(input.current().property("TEMP").value().toInteger());
});

// Create a timer to trigger the retirement of the "all" memory every 10 secs
stream.create().timer("monitor").interval().seconds(10).onTimer(function (timer) {
    stream.memory("all").checkLimit();
});

// Set the onMessage callback
stream.onMessage(function () {
    // Add current message to all memories
    stream.memory("all").add(stream.current());
    stream.memory("warning").add(stream.current());
    stream.memory("critical").add(stream.current());

    // Check if we have a warning condition (last 2 temps > 100)
    if (stream.memory("warning").select("temp > 100").size() == 2)
        stream.log().warning("LAST 2 TEMPS > 100!");

    // Check if we have a critical condition (spike):
    // At least 4 messages received,
    // First temp > 400,
    // All temps are greater than the temp before (ascendingSeries),
    // Last temp is 1.5 times greater than the first temp.
    if (stream.memory("critical").size() == 4 &&
        stream.memory("critical").first().property("temp").value().toDouble() > 400 &&
        stream.memory("critical").ascendingSeries("temp") &&
        stream.memory("critical").last().property("temp").value().toDouble() >
        stream.memory("critical").first().property("temp").value().toDouble() * 1.5) {

        stream.log().error("WE HAVE A SPIKE!!!");
        stream.mailserver(mailHost)
            .email()
            .from(mailFrom)
            .to(mailTo)
            .bcc(mailBcc)
            .subject("Nuclear Powerplant Monitor - CRITICAL!")
            .body("Temp spike detected, last temps=" + stream.memory("critical").values("temp"))
            .send();
    }

});
