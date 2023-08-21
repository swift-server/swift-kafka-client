//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
// For `strdup` and `free`
#if canImport(Glibc)
import Glibc
#else
import Darwin.POSIX
#endif

/// Wrapper type for initializing a `librdkafka` producer message
/// using the variadic argumetn initializer (required to set message headers).
internal class RDKafkaUnsafeProducerMessage {
    internal private(set) var _internal: [rd_kafka_vu_t]

    internal let size: Int

    init(
        topicHandle: OpaquePointer,
        partition: Int32,
        messageFlags: Int32,
        key: UnsafeRawBufferPointer?,
        value: UnsafeRawBufferPointer,
        opaque: UnsafeMutableRawPointer?,
        headers: [KafkaHeader]
    ) {
        var index = 0
        let sizeWithoutHeaders = (key != nil) ? 6 : 5
        self.size = sizeWithoutHeaders + headers.count
        self._internal = Array(repeating: rd_kafka_vu_t(), count: self.size)

        self._internal[index].vtype = RD_KAFKA_VTYPE_RKT
        self._internal[index].u.rkt = topicHandle
        index += 1

        self._internal[index].vtype = RD_KAFKA_VTYPE_PARTITION
        self._internal[index].u.i32 = partition
        index += 1

        self._internal[index].vtype = RD_KAFKA_VTYPE_MSGFLAGS
        self._internal[index].u.i = messageFlags
        index += 1

        if let key {
            self._internal[index].vtype = RD_KAFKA_VTYPE_KEY
            self._internal[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: key.baseAddress)
            self._internal[index].u.mem.size = key.count
            index += 1
        }

        self._internal[index].vtype = RD_KAFKA_VTYPE_VALUE
        self._internal[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: value.baseAddress)
        self._internal[index].u.mem.size = value.count
        index += 1

        self._internal[index].vtype = RD_KAFKA_VTYPE_OPAQUE
        self._internal[index].u.ptr = opaque
        index += 1

        for header in headers {
            self._internal[index].vtype = RD_KAFKA_VTYPE_HEADER

            // Copy name C String (strdup) as we do not own the headers: [KafkaHeader] array
            guard let headerNameCPointer = header.key.withCString(strdup) else {
                continue
            }
            self._internal[index].u.header.name = UnsafePointer(headerNameCPointer)

            if let value = header.value {
                // Copy value bytes as we do not own the headers: [KafkaHeader] array
                let valueCopyBuffer = UnsafeMutableRawPointer.allocate(byteCount: value.readableBytes, alignment: 1)
                value.withUnsafeReadableBytes { valueBuffer in
                    if let baseAddress = valueBuffer.baseAddress {
                        valueCopyBuffer.copyMemory(from: baseAddress, byteCount: valueBuffer.count)
                        self._internal[index].u.header.val = UnsafeRawPointer(valueCopyBuffer)
                        self._internal[index].u.header.size = value.readableBytes
                    }
                }
            }

            index += 1
        }

        assert(self._internal.count == self.size)
    }

    deinit {
        // Free resources allocated for specifying the header argument of the message
        for variadicArgument in self._internal {
            if variadicArgument.vtype == RD_KAFKA_VTYPE_HEADER {
                if let mutableNamePointer = UnsafeMutablePointer(mutating: variadicArgument.u.header.name) {
                    free(mutableNamePointer)
                }
                if let valuePointer = variadicArgument.u.header.val {
                    valuePointer.deallocate()
                }
            }
        }
    }
}
