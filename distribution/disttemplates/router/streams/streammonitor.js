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
 This stream monitors all streams registered at the stream registry and
 logs all messages received from them.
 */
var registryTopic = parameters.optional("registry-topic", "stream_" + stream.routerName() + "_streamregistry");


stream.create().input(stream.create().tempQueue("registryreply")).queue().onInput(function (input) {
    streamAvailable(input.current());
});

stream.create().input(registryTopic).topic().onInput(function (input) {
    if (!input.current().property("streamdata").exists())
        return;
    if (input.current().property("available").value().toBoolean())
        streamAvailable(input.current());
    else
        streamUnavailable(input.current());
});

function streamAvailable(message) {
    var body = JSON.parse(message.body());
    stream.log().info("Available: " + body.body.streamname + ", type: " + body.body.streamtype);
    stream.create().input(stream.create().tempQueue(body.body.streamname + "_reply")).queue().onInput(function (input) {
        stream.log().info(body.body.streamname + "/" + input.current().body());
    }).start();
    var out = stream.create().output(body.body.streamname).topic();
    out.send(
        stream.create()
            .message()
            .message()
            .property("initrequest").set(true)
            .replyTo(stream.tempQueue(body.body.streamname + "_reply").destination())
    );
    out.close();
    stream.create().input(body.body.streamname).topic().onInput(function (input) {
        if (!(input.current().property("streamdata").exists() || input.current().property("servicereply").exists()))
            return;
        stream.log().info(body.body.streamname + "/" + input.current().body());
    }).start();

}

function streamUnavailable(message) {
    var body = JSON.parse(message.body());
    stream.log().info("Unvailable: " + body.body.streamname + ", type: " + body.body.streamtype);
    stream.input(stream.tempQueue(body.body.streamname + "_reply").queueName()).close();
    stream.tempQueue(body.body.streamname + "_reply").delete();
    stream.input(body.body.streamname).close();
}

stream.onStart(function () {
    stream.create().output(registryTopic).topic();
    stream.output(registryTopic).send(
        stream.create()
            .message()
            .message()
            .property("initrequest").set(true)
            .replyTo(stream.tempQueue("registryreply").destination())
    );
    stream.output(registryTopic).close();
});

