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
var streamname = "stream_" + stream.routerName() + "_streamregistry";
var topic = parameters.optional("topic", streamname);

// Registry memory
stream.create().memory("registry").heap().createIndex("streamname");

// Registry Requests
stream.create().input("registryrequest").topic().destinationName(topic).selector("registryrequest = true")
    .onInput(function (input) {
        var avail = input.current().property("available").value().toBoolean();
        if (avail) {
            stream.memory("registry").add(input.current());
            send("streamavailable", stream.output(topic), input.current());
        }
        else {
            stream.memory("registry").index("streamname").remove(input.current().property("streamname").value().toString());
            send("streamunavailable", stream.output(topic), input.current());
        }
    });

// Send Available/Unavailable
function send(type, out, message) {
    var msg = {
        msgtype: type,
        body: {
            streamname: message.property("streamname").value().toString(),
            streamtype: message.property("streamtype").value().toString()
        }
    };
    out.send(
        stream.create().message().textMessage()
            .property("streamdata").set(true)
            .property("streamname").set(streamname)
            .property("available").set(type === "streamavailable")
            .body(JSON.stringify(msg))
    );
    stream.log().info(JSON.stringify(msg));
}

// Init Requests
// This must be a durable subscriber to ensure
// that initrequests from other routers are served
// while this stream has not yet started
stream.create().input("initrequests").topic()
    .durable()
    .clientId("streamregistry")
    .durableName("initrequests")
    .destinationName(topic)
    .selector("initrequest = true")
    .onInput(function (input) {
        var out = stream.create().output(null).forAddress(input.current().replyTo());
        stream.memory("registry").forEach(function (message) {
            send("streamavailable", out, message);
        });
        out.close();
    });

// Updates
stream.create().output(topic).topic();