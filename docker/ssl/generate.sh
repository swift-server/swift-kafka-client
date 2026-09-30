#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the swift-kafka-client open source project
##
## Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##
#
# Generates a self-signed test PKI for the SSL / SASL_SSL integration tests.
#
# TEST-ONLY MATERIAL. These certificates, keys, and passwords exist solely to
# exercise the TLS and SASL code paths against a local test broker. Do not use
# anywhere else.
#
# These files are generated fresh at container startup (docker-compose `cert-gen`
# service) into a shared volume; they are NOT committed. Pass an output directory as
# the first argument (defaults to this script's directory for ad-hoc local runs).
#
# The broker (Java, apache/kafka image) consumes PKCS#12 keystores plus credential
# files; the Swift client (librdkafka) consumes PEM (ca.crt, client.crt, client.key).

set -euo pipefail

OUTPUT_DIR="${1:-$(cd "$(dirname "$0")" && pwd)}"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

DAYS=3650
PASSWORD="test-password"

# 1. Certificate Authority — trust root shared by broker and client.
openssl req -x509 -newkey rsa:2048 -sha256 -days "$DAYS" -nodes \
    -keyout ca.key -out ca.crt -subj "/CN=swift-kafka-test-ca"

# 2. Broker certificate. The SAN must cover the hostname the client dials
#    (`kafka` in docker-compose, `localhost` otherwise) because librdkafka verifies
#    the broker hostname by default (ssl.endpoint.identification.algorithm=https).
openssl req -newkey rsa:2048 -nodes -keyout broker.key -out broker.csr \
    -subj "/CN=kafka"
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -sha256 -days "$DAYS" -out broker.crt \
    -extfile <(printf "subjectAltName=DNS:kafka,DNS:localhost")

# 3. Client certificate for mTLS (PEM). Its CN becomes the authenticated principal.
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
    -subj "/CN=swift-kafka-test-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -sha256 -days "$DAYS" -out client.crt

# 4. Broker keystore (PKCS#12): broker key (encrypted with $PASSWORD) + cert + CA chain.
openssl pkcs12 -export -in broker.crt -inkey broker.key -certfile ca.crt \
    -name kafka -passout "pass:$PASSWORD" -out broker.keystore.p12

# 5. Broker truststore (PKCS#12): the CA, so the broker can verify mTLS client certs.
rm -f broker.truststore.p12
keytool -importcert -alias ca -file ca.crt -keystore broker.truststore.p12 \
    -storetype PKCS12 -storepass "$PASSWORD" -noprompt

# 6. Credential files read by the apache/kafka image (must have no trailing newline).
printf '%s' "$PASSWORD" >keystore_creds
printf '%s' "$PASSWORD" >key_creds
printf '%s' "$PASSWORD" >truststore_creds

# 7. Server-side JAAS entry enabling SCRAM validation on the SASL_SSL listener
#    (credentials themselves live in cluster metadata, created by the kafka-setup service).
printf 'KafkaServer {\n    org.apache.kafka.common.security.scram.ScramLoginModule required;\n};\n' >kafka_jaas.conf

# Drop intermediate artifacts (the key/cert are now inside broker.keystore.p12).
rm -f broker.csr broker.crt broker.key client.csr ca.srl

echo "Generated test PKI in $(pwd):"
ls -1
