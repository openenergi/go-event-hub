FROM golang:1.8.5-stretch

RUN apt-get update 

# download Apache QPID
RUN apt-get install git
WORKDIR /go/src/
RUN git clone --progress --verbose http://git.apache.org/qpid-proton.git

# README:
# to check the version of a package: `dpkg -s <PACKAGE_NAME>`

# install the dependencies needed to compile Apache QPID
RUN apt-get install -y gcc=4:6.3.0-4 \
                       g++=4:6.3.0-4 \
                       cmake=3.7.2-1 \
                       cmake-curses-gui=3.7.2-1 \
                       uuid-dev=2.29.2-1

# SSL and Cyrus SASL requirements
RUN apt-get install -y libssl-dev=1.1.0f-3+deb9u1 \
                       libsasl2-2=2.1.27~101-g0780600+dfsg-3 \
                       libsasl2-dev=2.1.27~101-g0780600+dfsg-3 \
                       libsasl2-modules=2.1.27~101-g0780600+dfsg-3
RUN apt-get install -y swig=3.0.10-1.1

# compile Apache QPID proton-c
WORKDIR /go/src/qpid-proton
RUN git checkout tags/0.18.0
WORKDIR /go/src/qpid-proton/build
RUN cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DSYSINSTALL_BINDINGS=ON
RUN make install

# Apache QPID Go dependencies
# RUN go get qpid.apache.org/electron

# make sure the Go Electron dependency has the expected commit ID
# this is needed because of an issue with timestamps and Azure Event Hub
#
# for more details cf.:
# https://issues.apache.org/jira/browse/PROTON-1717
# https://github.com/apache/qpid-proton/commit/4edafb1a473e3a0d9aa3b9498a3f5bba257aba0a
WORKDIR /go/src/qpid-proton/proton-c/bindings/go/src/qpid.apache.org/
RUN git checkout 4edafb1a473e3a0d9aa3b9498a3f5bba257aba0a
RUN cp -r /go/src/qpid-proton/proton-c/bindings/go/src/qpid.apache.org /go/src/

# the source code of the internal dependency 
# the HTTP call to random is to make sure this clone is not a cached operation
WORKDIR /go/src/github.com/openenergi/
ADD http://www.random.org/strings/?num=1&len=10&digits=on&unique=on&format=plain&rnd=new uuid
RUN git clone https://github.com/openenergi/go-event-hub.git
