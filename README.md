# Go Event Hub

This is an Azure Event Hub AMQP 1.0 connector for the Go programming language (Golang) based on Apache Qpid Proton (an AMQP 1.0 C library).

# Installation and setup

You can follow the procedure described in the Ansible script to compile and install Apache Qpid Proton on a Linux Debian machine: `setup-amqp.yaml`.

To install the Go wrapper (Electron) on a Linux Debian machine you can follow the procedure described in the docker file: `oe-mseventhub.Dockerfile`. For more details about those tweaks keep reading further down this document.

## Requirements

- Go 1.8
- Apache Qpid Proton 0.18.0
- Go Electron 0.18.1 (more on this further down this document)

# Sample sender / receiver

Make sure you have configured the connection details of your Azure Event Hub inside the Go files, then compile the Go apps:

- for the sender: `go build -o sender mainSenderApp.go`
- for the receiver: `go build -o receiver mainReceiverApp.go`

After the compilation you can run them:

- the sender: `PN_TRACE_FRM=1 ./sender`
- the receiver: `PN_TRACE_FRM=1 ./receiver`

For more details on the Proton environment variables check: https://qpid.apache.org/releases/qpid-proton-0.18.0/proton/c/api/group__transport.html

# Tests

- Make sure the testing files are using the connection details
  of your Azure Event Hub.
- Run all the tests: `go test -v ./msauth/... ./eventhub/...`.

# Electron workaround

The `Electron` library needs to be fetched using `git` instead of `go get`
due to a timestamp issue and bug fix to be released on Proton versions after the current 0.18.1, the git commit ID is: `4edafb1a473e3a0d9aa3b9498a3f5bba257aba0a`.

For the context and more details on how to tweak `Electron` in your `${GOPATH}` check the procedure defined in this docker file: `oe-mseventhub.Dockerfile`.  

# Docker

## Base Proton/Electron image

- to build the base image: `docker build -t oe-mseventhub -f oe-mseventhub.Dockerfile .`

## Example of a sender

- build the image: `docker build -t eh-sender_i -f sender.Dockerfile .`;
- run the image: `docker stop eh-sender_c ; docker rm eh-sender_c ; docker run -d --name eh-sender_c -it eh-sender_i`;
- log into the container: `docker exec -it eh-sender_c bash`, 
  in there you can run the Go app like: `./sender` (or `PN_TRACE_FRM=1 ./sender`).

# Networking

To troubleshoot networking issues with Wireshark due to sockets, TSL connection, SSL keys, etc. you could use a filter like this: `ip.addr == 192.168.XXX.YYY && (tcp.srcport == 5671 || tcp.dstport == 5671)` where `5671` is the standard AMQP 1.0 TCP port to connect to the Azure Event Hub.

Make sure the `Debug` option of the Go instance representing creating `electron.Connection` is set to `true` so the SSL key is stored to a file to be later used by Wireshark to decrypt the network traffic capture.
Check `common.go` for more details on this.

# Further readings

## AMQP / Azure Event Hub

- Microsoft details about the Event Hub protocol on top of AMQP 1.0: https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide
- Go examples about sender/receiver via AMQP 1.0: https://github.com/apache/qpid-proton/tree/master/examples/go
- On the Event Hub partitions: https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features
- Electron, a high level QPid Proton Go wrapper: https://godoc.org/qpid.apache.org/electron (also coming with the AMQP package: https://godoc.org/qpid.apache.org/amqp and the Proton package: https://godoc.org/qpid.apache.org/proton)
- Azure Event hub AMQP message properties and filters: http://azure.github.io/amqpnetlite/articles/azure_eventhubs.html

## TLS / Wireshark

- How to store the TLS key in Go: https://golang.org/pkg/crypto/tls/#example_Config_keyLogWriter
- The TLS key follows this Mozilla NSS Key Log format: https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS/Key_Log_Format
