# Welcome to the SwiftMQ Community Edition (CE)

SwiftMQ CE is a full featured enterprise messaging system that supports:

- JMS 1.1
- AMQP 0.9.1 and 1.0
- MQTT 3.1 and 3.1.1
- Federated Router Networks
- Clustering

It is also the only messaging system with an integrated microservice platform called
_SwiftMQ Streams_ that runs inside a SwiftMQ Router and can be easily integrated into 
existing infrastructures and data feeds.
                       
Streams can intercept any message flow sent by applications over queues and topics. 
No need to change application code or messages.
                       
Streams communicate via Messages over queues and/or topics. They can be deployed on a 
single Router or on many different nodes of a Router Network. SwiftMQ's integrated 
Federated Router Network provides transparent message routing so that Streams on one 
node can communicate with Streams on other nodes transparently.

The data can be analyzed by using the realtime streaming analytics capabilities of the 
Streams Interface which are on par with popular realtime streaming analytics libraries. 
It provides sliding, tumbling and session windows, event time processing and late arrival 
handling, to name a few features.

## Documentation

Find the documentation [here](https://docs.swiftmq.com).

## Obtain SwiftMQ CE

### Release Distributions

You can download the distributions from the [Release Section](https://github.com/iitsoftware/swiftmq-ce/releases).

### Docker

SwiftMQ CE is on docker.io. Please use tag `latest` or the release number to pull it. Have a look at 
the documentation on how to configure the docker container.

## Building from Source

You need Apache Maven installed on your system. Then perform a 

    mvn clean install
    
which generates the `tar.gz` and `zip` distributions into the `distribution/target/` directory.

## Running the Test Suites

The contained test suite distribution can be used to test JMS and AMQP 1.0 functionality. It requires a started
SwiftMQ Router in standard configuration (just unpack, `cd scripts`, `./router`).

Unpack the test suite and `cd scripts`. Then run:

- `./runjms <path-to-router>`
- `./runamqp <path-to-router>`

`<path-to-router>` is the path to the SwiftMQ Router installation.

The test suite runs several hundred tests against the JMS API or the AMQP 1.0 protocol, respectively. 

## Community Support / Reporting Bugs

Please use the [Issue Tracker](https://github.com/iitsoftware/swiftmq-ce/issues) to file any bugs. 

## Contributing

We appreciate and welcome any contribution to this project. Please follow these guidelines:

### We use `git flow`

If you don't know it, read the [Tutorial](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) first.

Our main development branch (with all latest changes merged in) is `develop`. The `master` branch is for official
releases only. 

Create a new branch with `git flow` and commit your changes there. If you are ready, push your branch to this repository. We will
take care of the merge if and only if your changes are appropriate.

### License

All your contributions are under the Apache 2.0 License. If you create new files, please add the license header
at the top of the file.

## Get in Touch

Please visit our website [www.swiftmq.com](https://www.swiftmq.com).

## Copyright and Trademark

SwiftMQ is a product and (c) of IIT Software GmbH. SwiftMQ and Swiftlet are registered trademarks (R) of IIT Software GmbH.

