FROM oe-mseventhub

# make sure the required environment variables have been passed to the run command
ENV EH_TEST_NAMESPACE=${EH_TEST_NAMESPACE}
ENV EH_TEST_NAME=${EH_TEST_NAME}
ENV EH_TEST_SAS_POLICY_NAME=${EH_TEST_SAS_POLICY_NAME}
ENV EH_TEST_SAS_POLICY_KEY=${EH_TEST_SAS_POLICY_KEY}

# where the source code of the Go app (main func) is located
WORKDIR /go/src/github.com/openenergi/go-event-hub/
# you need to change the connection details inside `mainSenderApp.go` so then you can
# replace the main file with your local one loading it into the docker image
ADD mainSenderApp.go .
RUN go build -o sender mainSenderApp.go

CMD ["/bin/bash"]
