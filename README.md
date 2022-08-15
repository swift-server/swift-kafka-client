# 🚧WIP🚧: swift-kafka-gsoc

This package is currently under development as part of the Google Summer of Code 2022.
It aims to provide a library to interact with Apache Kafka.

## Installing Dependencies

Please make sure to have the [`librdkafka`](https://github.com/edenhill/librdkafka) library installed before building.

#### macOS

```bash
brew install librdkafka
```

#### Linux

```bash
apt-get install librdkafka-dev
```

## Building the Project

#### macOS

We rely on the `openssl` package, which is *keg-only*. This means that Swift Package Manager will not find it by default. To build, we have to explicitly make the `PKG_CONFIG_PATH` environment variable point to the location of our `openssl` installation.

##### Building from Command Line

```bash
env PKG_CONFIG_PATH=$(brew --prefix)/opt/openssl@1.1/lib/pkgconfig swift build
```

##### Opening & Building the Package in Xcode

> **Note**
>
> Please make sure that Xcode is **not** running already. Otherwise, Xcode will not open the project in the specified environment.

```bash
env PKG_CONFIG_PATH=$(brew --prefix)/opt/openssl@1.1/lib/pkgconfig xed .
```

#### Linux

```bash
swift build
```

## 🚧WIP🚧: Docker

As part of developing for Linux, we also provide a Docker environment for this package. It only contains a proof-of-concept program but will be expanded in the future.

To run the proof-of-concept program, use:

```bash
docker-compose -f docker/docker-compose.yaml run swift-kafka-gsoc
```
