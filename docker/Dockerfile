ARG swift_version=5.7
ARG ubuntu_version=jammy
ARG base_image=swift:$swift_version-$ubuntu_version
FROM $base_image

RUN apt-get update
RUN apt-get install libsasl2-dev -y
RUN apt-get install libssl-dev -y

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

# Download submodules and checkout at appropiate commits
RUN git submodule init
RUN git submodule update

CMD ["swift", "test"]
