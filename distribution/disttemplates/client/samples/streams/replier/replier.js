/**
 * Author: IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

// A simple echo service that creates an input on a temp queue where it receives its requests
// The temp queue is registered in JNDI so that JMS clients can look it up and send requests to it
stream.create().input(stream.create().tempQueue("requestQueue").registerJNDI()).queue();

stream.onMessage(function () {

    stream.create().output(null).forAddress(stream.current().replyTo())
        .send(stream.create().message().textMessage().correlationId(stream.current().messageId())
            .body("RE: " + stream.current().body())).close();
});
