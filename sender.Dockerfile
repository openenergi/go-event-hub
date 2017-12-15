FROM oe-mseventhub

# where the source code of the Go app (main func) is located
WORKDIR /go/src/github.com/openenergi/go-event-hub/
RUN go build -o sender mainSenderApp.go

CMD ["/bin/bash"]
