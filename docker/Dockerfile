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

WORKDIR /swift-kafka-gsoc

COPY . /swift-kafka-gsoc

CMD ["swift", "test"]
