# Go Event Hub

This is an Azure Event Hub AMQP 1.0 connector for the Go programming language (Golang) based on Apache Qpid Proton (an AMQP 1.0 C library).

[![GoDoc](https://godoc.org/github.com/openenergi/go-event-hub?status.svg)](https://godoc.org/github.com/openenergi/go-event-hub)
[![Build Status](https://travis-ci.org/openenergi/go-event-hub.svg?branch=feature%2Ftravis-ci)](https://travis-ci.org/openenergi/go-event-hub)
[![Go Report Card](https://goreportcard.com/badge/github.com/openenergi/go-event-hub)](https://goreportcard.com/report/github.com/openenergi/go-event-hub)

# Installation and setup

You can follow the procedure described in the Ansible script to compile and install Apache Qpid Proton on a Linux Debian machine: `setup-amqp.yaml`.

To install the Go wrapper (Electron) on a Linux Debian machine you can follow the procedure described in the docker file: `oe-mseventhub.Dockerfile`. For more details about those tweaks keep reading further down this document.

## Requirements

- Go 1.8
- Apache Qpid Proton 0.19.0
- Go Electron 0.19.0

# Sample sender / receiver

There are a pair of Go files containing a `main` function to demonstrate the usage of the library.
They could be use to send and receive messages via the Event Hub.

## Connection details

The Go files expect to find some environment variables. Make sure you have configured the connection details of your Azure Event Hub e.g.:

```sh
export EH_TEST_NAMESPACE=foo
export EH_TEST_NAME=foo
export EH_TEST_SAS_POLICY_NAME=foo
export EH_TEST_SAS_POLICY_KEY=foo
export EH_TEST_CONSUMER_GROUP="\$Default"
```

## Compile

To compile the Go apps:

- for the sender: `go build -o sender mainSenderApp.go`
- for the receiver: `go build -o receiver mainReceiverApp.go`

## Run the sender

Note: the sender does not need `EH_TEST_CONSUMER_GROUP` to be setup. To run the sender: `PN_TRACE_FRM=1 ./sender`

## Run the receiver

To run the receiver: `PN_TRACE_FRM=1 ./receiver`

For more details on the Proton environment variables check: https://qpid.apache.org/releases/qpid-proton-0.18.0/proton/c/api/group__transport.html

# Tests

- Make sure you set the Event Hub connection details as environment variables as previously explained.
- Run all the tests: `go test -v ./msauth/... ./eventhub/...`.

For a complete live end to end integration test involving a receiver configured with a partition offset as `@latest`,
and a sender sending a message to the Event Hub, then listening to the receiver channel for the message previously sent
take a look at `end_to_end_integration_test.go`.

# Docker

## Base Proton/Electron image

- Make sure the connection details of the Event Hub are exported as environment variables (as explained before).
- To build the base image (and **run the tests** within within the building process): `docker build -t oe-mseventhub --build-arg EH_TEST_NAMESPACE=$EH_TEST_NAMESPACE --build-arg EH_TEST_NAME=$EH_TEST_NAME --build-arg EH_TEST_SAS_POLICY_NAME=$EH_TEST_SAS_POLICY_NAME --build-arg EH_TEST_SAS_POLICY_KEY=$EH_TEST_SAS_POLICY_KEY -f oe-mseventhub.Dockerfile .`.

## Example of a sender

- Build the docker image: `docker build -t eh-sender_i -f sender.Dockerfile .`.
- Make sure the connection details of the Event Hub are exported as environment variables (as explained before).
- Then run the docker container: `docker stop eh-sender_c ; docker rm eh-sender_c ; docker run -d --name eh-sender_c -e EH_TEST_NAMESPACE=$EH_TEST_NAMESPACE -e EH_TEST_NAME=$EH_TEST_NAME -e EH_TEST_SAS_POLICY_NAME=$EH_TEST_SAS_POLICY_NAME -e EH_TEST_SAS_POLICY_KEY=$EH_TEST_SAS_POLICY_KEY -it eh-sender_i`.
- You could then log into the container: `docker exec -it eh-sender_c bash`, 
  in there you can run the Go app like: `./sender` (or `PN_TRACE_FRM=1 ./sender`).

# Networking

To troubleshoot networking issues with Wireshark due to sockets, TSL connection, SSL keys, etc. you could use a filter like this: `ip.addr == 192.168.XXX.YYY && (tcp.srcport == 5671 || tcp.dstport == 5671)` where `5671` is the standard AMQP 1.0 TCP port to connect to the Azure Event Hub.

Make sure the `Debug` option of the Go instance representing an AMQP connection (`eventhub.SenderOpts` and `eventhub.ReceiverOpts`) is set to `true` so the SSL key is stored to a file to be later used by Wireshark to decrypt the network traffic capture.
Check `common.go` for more details on this.

# Further readings

## AMQP / Azure Event Hub

- Microsoft details about the Event Hub protocol on top of AMQP 1.0: https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide
- Go examples about sender/receiver via AMQP 1.0: https://github.com/apache/qpid-proton/tree/master/examples/go
- On the Event Hub partitions: https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features
- Electron, a high level Apache Qpid Proton Go wrapper: https://godoc.org/qpid.apache.org/electron (also coming with the AMQP package: https://godoc.org/qpid.apache.org/amqp and the Proton package: https://godoc.org/qpid.apache.org/proton)
- Azure Event hub AMQP message properties and filters: http://azure.github.io/amqpnetlite/articles/azure_eventhubs.html

## TLS / Wireshark

- How to store the TLS key in Go: https://golang.org/pkg/crypto/tls/#example_Config_keyLogWriter
- The TLS key follows this Mozilla NSS Key Log format: https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS/Key_Log_Format
