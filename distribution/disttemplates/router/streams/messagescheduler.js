/**
 * Copyright 2017 IIT Software GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a Message Scheduler that sends Messages with a delay to a given
 * Destination.
 */
var inputQueue = parameters.optional("input-queue", "streams_scheduler_input");
var storeQueue = parameters.optional("store-queue", "streams_scheduler_store");
var delayFormat = parameters.optional("delay-format", "dd.MM.yyyy HH:mm:ss");
var interval = typeconvert.toInteger(parameters.optional("interval", "10"));
var purgeInterval = typeconvert.toInteger(parameters.optional("purge-interval", "2"));
var maxBatchSize = typeconvert.toInteger(parameters.optional("max-batch-size", "10000"));

// Create input & store queue if not exists
stream.cli().execute("cc /sys$queuemanager/queues")
    .exceptionOff()
    .execute("new " + inputQueue)
    .execute("new " + storeQueue);

// Create the Input to receive schedule requests
stream.create().input(inputQueue).queue().onInput(function (input) {
    if (isValid(input.current()))
        computeDelayAndStore(input.current());
    else
        stream.log().error("Input message missed schedule property: " + input.current());
});

// Create the Memory to store the schedules
stream.create().memory("schedules").queue(storeQueue).orderBy("_TIME");

// Create the Timer to send scheduled Messages
stream.create().timer("scheduler").interval().seconds(interval).onTimer(function (timer) {
    var mem = stream.memory("schedules");
    if (mem.size() == 0)
        return;
    var currentTime = time.currentTime();
    var max = maxBatchSize;
    while (max-- > 0 && mem.size() > 0 && (msg = mem.first()) != null && msg.property("_TIME").value().toLong() <= currentTime) {
        sendToDestination(msg);
        mem.remove(0);
    }
});

// Create the Timer to purge unused Outputs
stream.create().timer("purger").interval().minutes(purgeInterval).onTimer(function (timer) {
    stream.purgeOutputs();
});

// Checks whether the Message contains all necessary Properties for schedulung
function isValid(message) {
    return message.property("streams_scheduler_delay").exists() &&
        message.property("streams_scheduler_destination").exists() &&
        message.property("streams_scheduler_destination_type").exists() ||
        message.property("JMS_SWIFTMQ_SCHEDULER_DESTINATION").exists() &&
        message.property("JMS_SWIFTMQ_SCHEDULER_DESTINATION_TYPE").exists() &&
        message.property("JMS_SWIFTMQ_SCHEDULER_TIME_EXPRESSION").exists() &&
        message.property("JMS_SWIFTMQ_SCHEDULER_DATE_FROM").exists();

}

// Computes the delay (old & new style) and stores the Message in the Memory
function computeDelayAndStore(message) {
    if (message.property("streams_scheduler_delay").exists()) {

        // New style
        var delay = message.property("streams_scheduler_delay").value().toString();
        try {
            message.property("_TIME")
                .set(time.parse(delay, delayFormat));
            stream.memory("schedules").add(message);
        } catch (e) {
            stream.log().error("Exception parsing delay time (" + delay + "): " + e);
        }

    } else if (message.property("JMS_SWIFTMQ_SCHEDULER_TIME_EXPRESSION").exists()) {

        // Old style (sys$scheduler)
        var expression = message.property("JMS_SWIFTMQ_SCHEDULER_TIME_EXPRESSION").value().toString();
        var delay;
        try {
            var date = message.property("JMS_SWIFTMQ_SCHEDULER_DATE_FROM").value().toString();
            if (date === 'now')
                date = time.format(time.currentTime(), "yyyy-MM-dd");
            if (expression.length === 8)
                delay = time.parse(expression + " " + date, "'at' HH:mm yyyy-MM-dd");
            else
                delay = time.parse(expression + " " + date, "'at' HH:mm:ss yyyy-MM-dd");
            message.property("_TIME").set(delay);
            stream.memory("schedules").add(message);
        } catch (e) {
            stream.log().error("Exception parsing time expression (" + expression + "): " + e);
        }


    }
}

// Create the Output according to the Message Properties
function createOutput(message) {
    var name;
    var type;
    if (message.property("streams_scheduler_destination").exists()) {
        name = message.property("streams_scheduler_destination").value().toString();
        type = message.property("streams_scheduler_destination_type").value().toString();
    } else {
        name = message.property("JMS_SWIFTMQ_SCHEDULER_DESTINATION").value().toString();
        type = message.property("JMS_SWIFTMQ_SCHEDULER_DESTINATION_TYPE").value().toString();
    }
    if (stream.output(name) === null) {
        if (type === 'queue')
            stream.create().output(name).queue();
        else
            stream.create().output(name).topic();
    }
    return stream.output(name);
}

// Removes all Scheduler Properties
function removeSchedulerProps(message) {
    message.properties().startsWith("streams_scheduler").forEach(function (prop) {
        prop.set(null);
    });
    message.properties().startsWith("JMS_SWIFTMQ_SCHEDULER").forEach(function (prop) {
        prop.set(null);
    });
    message.property("_TIME").set(null);
    return message;
}

// If necessary, sets the final Message expiration
function setFinalExpiration(message) {
    var expiration = -1;
    if (message.property("streams_scheduler_expiration").exists())
        expiration = message.property("streams_scheduler_expiration").value().toLong();
    else if (message.property("JMS_SWIFTMQ_SCHEDULER_EXPIRATION").exists())
        expiration = message.property("JMS_SWIFTMQ_SCHEDULER_EXPIRATION").value().toLong();
    if (expiration > 0)
        message.expiration(time.currentTime() + expiration);
    return message;
}

// Sends the Message to the final Destination
function sendToDestination(message) {
    var output = createOutput(message);
    try {
        setFinalExpiration(message);
        removeSchedulerProps(message);
        output.send(message);
    } catch (e) {
        stream.log().error("Error during sent to destination '" + output.name() + "': " + e);
        output.close();
    }

}
