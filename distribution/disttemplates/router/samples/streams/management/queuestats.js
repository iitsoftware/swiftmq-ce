/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */

// Create a ManagementInput on sys$queuemanager/usage and receive adds and changes.
// No need to set a onRemove callback as these Memories time out by the inactivity timeout.
// The selector filters those queue which are not system queues and not our own queue (qs_)
// and the message must contain the messagecount property (other property changes are not of interest).
stream.create().input("sys$queuemanager/usage")
    .management()
    .selector("name not like '%$%' and name not like 'qs\_%' and messagecount is not null")
    .onAdd(function (input) {
        // A new queue has been created.
        // Add it to the memory group.
        stream.memoryGroup("queues").add(input.current());
    })
    .onChange(function (input) {
        // Property "messagecount" has changed.
        // Add it to the MemoryGroup
        stream.memoryGroup("queues").add(input.current());
    });

// Create a MemoryGroup for all queues and register a callback for new groups
stream.create().memoryGroup("queues", "name").inactivityTimeout().minutes(2).onCreate(function (key) {
    // This is an example on how to use these callbacks. You can create whatever type of Memories.
    // Here we create a QueueMemory
    var queueName = "qs_" + key;

    // We need to create a regular queue as a backstore for the QueueMemory
    stream.cli()
        .execute("cc sys$queuemanager/queues")
        .exceptionOff()  // Queue might be already defined
        .execute("new " + queueName)
        .execute("save");

    // A new value of the group is received and we create a Memory for it
    // with a 1 min tumbling time window
    stream.create().memory(queueName).queue(queueName).limit().time().tumbling().minutes(1).onRetire(function (retired) {
        // Window is closed, we print the average message count over the last minute
        print("Queue " + key + ", avg backlog last minute = " + retired.average("messagecount"));
    });

    // Return the new memory
    return stream.memory(queueName);
}).onRemove(function (key) {
    // Memory has been removed. Delete the corresponding persistent queue
    stream.cli()
        .execute("cc sys$queuemanager/queues")
        .execute("delete qs_" + key)
        .execute("save");
});

// Create a timer that calls checkLimit on the memory group each minute
stream.create().timer("queues").interval().minutes(1).onTimer(function (timer) {
    stream.memoryGroup("queues").checkLimit();
});
