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

var streamname = "stream_" + stream.routerName() + "_routeannouncer";
var topic = parameters.optional("topic", streamname);

// Management Input to get the active routes
stream.create().input("sys$routing/usage/routing-table").management()
    .onAdd(function (input) {
        stream.memory("active-routes").add(input.current());
        sendUpdate(stream.output(topic), true, input.current().property("name").value().toString());
    })
    .onRemove(function (input) {
        stream.memory("active-routes").index("name").remove(input.current().property("name").value().toString());
        sendUpdate(stream.output(topic), false, input.current().property("name").value().toString());
    });

function sendUpdate(output, available, routername) {
    output.send(
        stream.create().message()
            .textMessage()
            .property("streamdata").set(true)
            .property("streamname").set(streamname)
            .body(JSON.stringify({
                msgtype: available ? "routeravailable" : "routerunavailable",
                body: {routername: routername}
            }))
    );
}

// Memory for active routes
stream.create().memory("active-routes").heap().createIndex("name");

// Init Requests
// This must be a durable subscriber to ensure
// that initrequests from clients are served
// while this stream has not yet been started
stream.create().input(topic).topic().selector("initrequest = true")
    .durable()
    .clientId("routeannouncer")
    .durableName("initrequests")
    .destinationName(topic)
    .onInput(function (input) {
        var out = stream.create().output(null).forAddress(input.current().replyTo());
        // Local router is always announced
        sendUpdate(out, true, stream.routerName());
        stream.memory("active-routes").forEach(function (message) {
            sendUpdate(out, true, message.property("name").value().toString());
        });
        out.close();
    });

// Updates
stream.create().output(topic).topic();