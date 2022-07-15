ARG swift_version=5.6
ARG ubuntu_version=bionic
ARG base_image=swift:$swift_version-$ubuntu_version
FROM --platform=linux/x86_64 $base_image

# dependencies
RUN apt-get update && apt-get install -y librdkafka-dev

WORKDIR /swift-kafka-gsoc

COPY . /swift-kafka-gsoc

CMD ["swift", "run", "ErrorPrinter"]
