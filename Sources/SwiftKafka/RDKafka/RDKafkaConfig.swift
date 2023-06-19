//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Logging

/// A collection of helper functions wrapping common `rd_kafka_conf_*` functions in Swift.
struct RDKafkaConfig {
    typealias KafkaAcknowledgementResult = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    /// Wraps a Swift closure inside of a class to be able to pass it to `librdkafka` as an `OpaquePointer`.
    final class CapturedClosures {
        typealias DeliveryReportClosure = (KafkaAcknowledgementResult?) -> Void
        var deliveryReportClosure: DeliveryReportClosure?

        typealias LoggingClosure = (Int32, UnsafePointer<CChar>, UnsafePointer<CChar>) -> Void
        var loggingClosure: LoggingClosure?

        init() { }
    }

    /// Create a new `rd_kafka_conf_t` object in memory and initialize it with the given configuration properties.
    /// - Parameter configDictionary: A dictionary containing the Kafka client configurations.
    /// - Returns: An `OpaquePointer` pointing to the newly created `rd_kafka_conf_t` object in memory.
    /// - Throws: A ``KafkaError`` if setting a config value failed.
    static func createFrom(configDictionary: [String: String]) throws -> OpaquePointer {
        let configPointer: OpaquePointer = rd_kafka_conf_new()
        try configDictionary.forEach { key, value in
            try Self.set(configPointer: configPointer, key: key, value: value)
        }

        return configPointer
    }

    /// A Swift wrapper for `rd_kafka_conf_set`.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter key: The configuration property to be changed.
    /// - Parameter value: The new value of the configuration property to be changed.
    /// - Throws: A ``KafkaError`` if setting the value failed.
    static func set(configPointer: OpaquePointer, key: String, value: String) throws {
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_conf_set(
            configPointer,
            key,
            value,
            errorChars,
            KafkaClient.stringSize
        )

        if configResult != RD_KAFKA_CONF_OK {
            let errorString = String(cString: errorChars)
            throw KafkaError.config(reason: errorString)
        }
    }

    /// Registers passed closures as callbacks and sets the application's opaque pointer that will be passed to callbacks
    /// - Parameter type: Kafka client type: `Consumer` or `Producer`
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter deliveryReport: A closure that is invoked upon message acknowledgement.
    /// - Parameter logger: Logger instance
    /// - Returns: A ``CapturedClosures`` object that must me retained by the caller as long as it exists.
    static func setCallbackClosures(
        type: RDKafka.ClientType,
        configPointer: OpaquePointer,
        deliveryReport: CapturedClosures.DeliveryReportClosure? = nil,
        logger: Logger
    ) -> CapturedClosures {
        let closures = CapturedClosures()

        // Pass the the reference to Opaque as an opaque object
        let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(closures).toOpaque()
        rd_kafka_conf_set_opaque(
            configPointer,
            opaquePointer
        )

        // Set delivery report callback
        if let deliveryReport, type == .producer {
            Self.setDeliveryReportCallback(configPointer: configPointer, capturedClosures: closures, deliveryReport)
        }
        // Set logging callback
        Self.setLoggingCallback(configPointer: configPointer, capturedClosures: closures, logger: logger)

        return closures
    }

    /// A Swift wrapper for `rd_kafka_conf_set_dr_msg_cb`.
    /// Defines a function that is called upon every message acknowledgement.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter callback: A closure that is invoked upon message acknowledgement.
    private static func setDeliveryReportCallback(
        configPointer: OpaquePointer,
        capturedClosures: CapturedClosures,
        _ deliveryReport: @escaping RDKafkaConfig.CapturedClosures.DeliveryReportClosure
    ) {
        capturedClosures.deliveryReportClosure = deliveryReport

        // Create a C closure that calls the captured closure
        let callbackWrapper: (
            @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
        ) = { _, messagePointer, opaquePointer in
            guard let opaquePointer = opaquePointer else {
                fatalError("Could not resolve reference to CapturedClosures")
            }
            let closures = Unmanaged<CapturedClosures>.fromOpaque(opaquePointer).takeUnretainedValue()

            if let actualCallback = closures.deliveryReportClosure {
                let messageResult = Self.convertMessageToAcknowledgementResult(messagePointer: messagePointer)
                actualCallback(messageResult)
            }
        }

        rd_kafka_conf_set_dr_msg_cb(
            configPointer,
            callbackWrapper
        )
    }

    /// A Swift wrapper for `rd_kafka_conf_set_log_cb`.
    /// Defines a function that is called upon every log and redirects output to ``logger``.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter logger: Logger instance
    private static func setLoggingCallback(
        configPointer: OpaquePointer,
        capturedClosures: CapturedClosures,
        logger: Logger
    ) {
        let loggingClosure: RDKafkaConfig.CapturedClosures.LoggingClosure = { level, fac, buf in
            // Mapping according to https://en.wikipedia.org/wiki/Syslog
            switch level {
            case 0 ... 2: /* Emergency, Alert, Critical */
                logger.critical(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            case 3: /* Error */
                logger.error(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            case 4: /* Warning */
                logger.warning(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            case 5: /* Notice */
                logger.notice(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            case 6: /* Informational */
                logger.info(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            default: /* Debug */
                logger.debug(Logger.Message(stringLiteral: String(cString: buf)), source: String(cString: fac))
            }
        }
        capturedClosures.loggingClosure = loggingClosure

        let loggingWrapper: (
            @convention(c) (OpaquePointer?, Int32, UnsafePointer<CChar>?, UnsafePointer<CChar>?) -> Void
        ) = { rkKafkaT, level, fac, buf in
            guard let fac, let buf else {
                return
            }

            guard let opaquePointer = rd_kafka_opaque(rkKafkaT) else {
                fatalError("Could not resolve reference to CapturedClosures")
            }
            let opaque = Unmanaged<CapturedClosures>.fromOpaque(opaquePointer).takeUnretainedValue()

            guard let closure = opaque.loggingClosure else {
                fatalError("Could not resolve logger instance")
            }
            closure(level, fac, buf)
        }

        rd_kafka_conf_set_log_cb(
            configPointer,
            loggingWrapper
        )
    }

    /// Convert an unsafe`rd_kafka_message_t` object to a safe ``KafkaAcknowledgementResult``.
    /// - Parameter messagePointer: An `UnsafePointer` pointing to the `rd_kafka_message_t` object in memory.
    /// - Returns: A ``KafkaAcknowledgementResult``.
    private static func convertMessageToAcknowledgementResult(
        messagePointer: UnsafePointer<rd_kafka_message_t>?
    ) -> KafkaAcknowledgementResult? {
        guard let messagePointer else {
            return nil
        }

        let messageID = UInt(bitPattern: messagePointer.pointee._private)

        let messageResult: KafkaAcknowledgementResult
        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)
            messageResult = .success(message)
        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }
            messageResult = .failure(error)
        }

        return messageResult
    }
}
