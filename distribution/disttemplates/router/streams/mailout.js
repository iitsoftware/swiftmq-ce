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

var inputQueue = parameters.optional("input-queue", "streams_mailout");
var serverName = parameters.optional("servername", "localhost");
var userName = parameters.require("username");
var password = parameters.require("password");
var defaultFrom = parameters.get("default-from");
var defaultTo = parameters.get("default-to");
var defaultCC = parameters.get("default-cc");
var defaultBCC = parameters.get("default-bcc");
var defaultSubject = parameters.get("default-subject");

// Create input queue if not exists
stream.cli().execute("cc /sys$queuemanager/queues")
    .exceptionOff()
    .execute("new " + inputQueue);

stream.create()
    .mailserver(serverName)
    .username(userName)
    .password(password)
    .connect();

stream.create().input(inputQueue).queue().onInput(function (input) {
    var email = stream.mailserver(serverName).email();
    var valid = false;
    try {
        validate(email, input.current(), "from", defaultFrom, true);
        validate(email, input.current(), "to", defaultTo, true);
        validate(email, input.current(), "cc", defaultCC, false);
        validate(email, input.current(), "bcc", defaultBCC, false);
        validate(email, input.current(), "subject", defaultSubject, true);
        email.body(input.current().body());
        valid = true;
    } catch (e) {
        stream.log().error(e);
    }
    if (valid) // Necessary to put it outside the try/catch because of the restart semantic
        email.send();
});

function validate(email, message, field, defaultField, required) {
    if (message.property(field).exists())
        email.set(field, message.property(field).value().toString());
    else if (defaultField != null)
        email.set(field, defaultField);
    else if (required)
        throw "Neither set in the input message nor specified as default: " + field;
}