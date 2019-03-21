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
var inputQueue = parameters.optional("input-queue", "streamrepo");
var storeQueue = parameters.optional("store-queue", "streamrepo_store");

// Create input & store queue if not exists
stream.cli().execute("cc /sys$queuemanager/queues")
    .exceptionOff()
    .execute("new " + inputQueue)
    .execute("new " + storeQueue);

// Command input
stream.create().input(inputQueue).queue().onInput(function (input) {
    var out = stream.create().output(null).forAddress(input.current().replyTo());
    var result;
    var operation = input.current().property("operation").value().toString();
    switch (operation) {
        case 'add':
            result = add(input.current());
            break;
        case 'remove':
            result = remove(input.current());
            break;
        case 'list':
            result = list(input.current());
            break;
        default:
            break;
    }
    out.send(result);
    out.close();
});

// Memory group to store the repos per app
stream.create().memoryGroup("repo", "app").onCreate(function (key) {
    stream.create().memory(key).sharedQueue(storeQueue).createIndex("file");
    return stream.memory(key);
});

// Register everything on start
stream.onStart(function () {
  stream.memoryGroup("repo").forEach(function (memory) {
    memory.forEach(function (message) {
      var key = repoKey(message.property("app").value().toString(), message.property("file").value().toString());
      repository.put(key, message.body());
    });
  });
});

// Remove everything on stop
stream.onStop(function () {
  stream.memoryGroup("repo").forEach(function (memory) {
    memory.forEach(function (message) {
      var key = repoKey(message.property("app").value().toString(), message.property("file").value().toString());
      repository.remove(key);
    });
  });
});

function repoKey(appname, scriptname) {
  return "repository:/" + appname + "/" + scriptname;
}

function add(message) {
  var key = repoKey(message.property("app").value().toString(), message.property("file").value().toString());
    var mem = stream.memory(message.property("app").value().toString());
    if (mem) {
        mem.index("file").remove(message.property("file").value().toString());
      repository.remove(key);
    }
    message.property("storetime").set(time.currentTime());
    stream.memoryGroup("repo").add(message);
  repository.put(key, message.body());
    var resMsg = stream.create().message().mapMessage();
    resMsg.body()
        .set("success", true)
        .set("operation", "add")
        .set("app", message.property("app").value().toString())
        .set("storetime", time.format(message.property("storetime").value().toLong(), "dd.MM.yyyy HH:mm:ss"));
    return resMsg;
}

function remove(message) {
    var success = false;
    var result = "";
    var mem = stream.memory(message.property("app").value().toString());
    if (mem) {
        if (message.property("file").exists()) {
            if (mem.index("file").get(message.property("file").value().toString()).size() > 0) {
                mem.index("file").remove(message.property("file").value().toString());
              repository.remove(repoKey(message.property("app").value().toString(), message.property("file").value().toString()));
                success = true;
            }
            else
                result = "File '" + message.property("file").value().toString() + "' does not exists in repository '" + message.property("app").value().toString() + "'";
        } else {
            mem.forEach(function (msg) {
              repository.remove(repoKey(msg.property("app").value().toString(), msg.property("file").value().toString()));
            });
            mem.clear();
            stream.memoryGroup("repo").removeMemory(message.property("app").value().toString());
            mem.close();
            success = true;
        }
    } else
        result = "Repository '" + message.property("app").value().toString() + "' does not exists";
    var resMsg = stream.create().message().mapMessage();
    resMsg.body()
        .set("success", success)
        .set("operation", "remove")
        .set("app", message.property("app").value().toString())
        .set("result", result);
    return resMsg;
}

function list(message) {
    var success = false;
    var result = "";
    var list = [];
    var mem = stream.memory(message.property("app").value().toString());
    if (mem) {
        mem.forEach(function (msg) {
            list.push({
                file: msg.property("file").value().toString(),
                storetime: msg.property("storetime").value().toLong()
            })
        });
        success = true;
    } else
        result = "Repository '" + message.property("app").value().toString() + "' does not exists";
    var resMsg = stream.create().message().mapMessage();
    resMsg.body()
        .set("success", success)
        .set("operation", "list")
        .set("app", message.property("app").value().toString())
        .set("result", result)
        .set("nfiles", "" + list.length);
    for (var i = 0; i < list.length; i++) {
        resMsg.body().set("file" + i, list[i].file).set("storetime" + i, time.format(list[i].storetime, "dd.MM.yyyy HH:mm:ss"))
    }
    return resMsg;
}
