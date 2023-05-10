ARG swift_version=5.7
ARG ubuntu_version=jammy
ARG base_image=swift:$swift_version-$ubuntu_version
FROM $base_image

# Use Confluent repository for downloading librdkafka-dev because
# apt-get only supports librdkafka-dev version 1.2.1
RUN apt-get update
RUN apt-get install wget software-properties-common gnupg -y
RUN wget -qO - http://packages.confluent.io/deb/7.2/archive.key | apt-key add -
RUN add-apt-repository "deb http://packages.confluent.io/deb/ $(lsb_release -cs) main" -y
RUN apt-get install librdkafka-dev -y

# tools
RUN mkdir -p $HOME/.tools
RUN echo 'export PATH="$HOME/.tools:$PATH"' >> $HOME/.profile

# swiftformat (until part of the toolchain)
ARG swiftformat_version=0.51.8
RUN git clone --branch $swiftformat_version --depth 1 https://github.com/nicklockwood/SwiftFormat $HOME/.tools/swift-format
RUN cd $HOME/.tools/swift-format && swift build -c release
RUN ln -s $HOME/.tools/swift-format/.build/release/swiftformat $HOME/.tools/swiftformat

WORKDIR /swift-kafka-gsoc

COPY . /swift-kafka-gsoc

CMD ["swift", "test"]
