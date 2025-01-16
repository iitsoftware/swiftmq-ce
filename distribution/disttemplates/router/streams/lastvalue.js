/*
 * Copyright 2025 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

let lastValueProp = parameters.optional("last-value-property", "swiftmq_lastvalue_property");
let lastValueValue = parameters.optional("last-value-value", "swiftmq_lastvalue_value");
let expirationMS = parameters.optional("expiration", "-1");
let persistent = parameters.optional("persistent", "false");
let topicName = parameters.require("topic-name");
let verbose = parameters.optional("verbose", "false") === "true";
let inputTopic = topicName + ".in";
let outputTopic = topicName + ".out";
let storeQueue = parameters.optional("store-queue", "none");
const memoryName = "lastvalues";
const subscribers = {};

if (persistent === "true" && storeQueue === "none")
    throw "You need to specify the 'store-queue' parameter if parameter 'persistent' is 'true'."

// Create shared queue if not exists
if (persistent === "true") {
    stream.cli().execute("cc /sys$queuemanager/queues")
        .exceptionOff()
        .execute("new " + storeQueue);
}

getOrCreateMemory(Number(expirationMS), persistent === "true");

stream.create().timer("checklimit").interval().seconds(30).onTimer(timer => {
    stream.memory(memoryName).checkLimit();
});

stream.log().info("Send last value messages to topic: " + inputTopic);
stream.log().info("Receive last value messages from topic: " + outputTopic);

stream.create().input(inputTopic).topic().onInput(input => {
    processMessage(input.current());
});

// Management Input to get subscribers on outputTopic
stream.create().input("sys$topic/usage/subscriber").management().selector("topic = '" + outputTopic + "'")
    .onAdd(function (input) {
        const name = input.current().property("name").value().toString();
        const boundTo = input.current().property("boundto").value().toString();
        if (verbose) stream.log().info("New subscriber on topic '" + outputTopic + "', name=" + name + ", boundTo=" + boundTo);
        addSubscriber(name, boundTo);
    })
    .onRemove(function (input) {
        const name = input.current().property("name").value().toString();
        const boundTo = input.current().property("boundto").value().toString();
        if (verbose) stream.log().info("Remove subscriber from topic '" + outputTopic + "', name=" + name + ", boundTo=" + boundTo);
        removeSubscriber(name);
    });

function addSubscriber(name, boundTo) {
    subscribers[name] = stream.create().output(boundTo).queue();
    stream.memory(memoryName).forEach(message => {
        subscribers[name].send(message);
    })
}

function removeSubscriber(name) {
    subscribers[name].close();
    delete subscribers[name];
}

function sendUpdate(message) {
    for (const name in subscribers) {
        subscribers[name].send(message);
    }
}

function processMessage(message) {
    const propertyExists = message.property(lastValueProp).exists();
    const valueExists = message.property(lastValueValue).exists();

    // Check if property exists and process accordingly
    if (propertyExists && valueExists) {
        const key = message.property(lastValueProp).value().toObject();
        const value = message.property(lastValueValue).value().toObject();
        if (verbose) stream.log().info(`Adding: key: ${key}, value: ${value}`);

        const memory = stream.memory(memoryName);
        memory.index(lastValueProp).remove(key);
        memory.add(message);
        sendUpdate(message);
    } else {
        stream.log().warning(`Invalid message received: property '${lastValueProp}' exists: ${propertyExists}, property '${lastValueValue}' exists: ${valueExists}`);
    }
}


function getOrCreateMemory(expirationMS, persistent) {
    if (stream.memory(memoryName) === null) {
        if (persistent)
            stream.create().memory(memoryName).queue(storeQueue).createIndex(lastValueProp);
        else
            stream.create().memory(memoryName).heap().createIndex(lastValueProp);
        if (expirationMS === -1) {
            stream.log().info(`Create memory without expiration, persistent: ${persistent}`);
        } else {
            stream.log().info(`Create memory with ${expirationMS} ms expiration, persistent: ${persistent}`)
            const memory = stream.memory(memoryName);
            memory.limit().time().tumbling().milliseconds(expirationMS)
                .onRetire(retired => retired.forEach(message => {
                    const key = message.property(lastValueProp).value().toObject();
                    const value = message.property(lastValueValue).value().toObject();
                    if (verbose) stream.log().info(`Expired: ${key} = ${value}`);
                }));
        }
    }
    return stream.memory(memoryName);
}
