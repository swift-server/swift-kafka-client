ARG swift_version=5.7
ARG ubuntu_version=focal
ARG base_image=swift:$swift_version-$ubuntu_version
FROM $base_image

# Build librdkafka from source because apt-get
# only supports librdkafka-dev version 1.2.1

# Install librdkafka dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    gcc \
    libzstd-dev \
    libsasl2-dev \
    libssl-dev \
    zlib1g-dev
RUN git clone --depth 1 https://github.com/edenhill/librdkafka
RUN cd librdkafka && ./configure && make && make install && ldconfig

WORKDIR /swift-kafka-gsoc

COPY . /swift-kafka-gsoc

CMD ["swift", "test"]
