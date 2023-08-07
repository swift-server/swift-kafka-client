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

#if swift(<5.9)
@preconcurrency import Foundation
#else
import Foundation
#endif
import Kafka

#if swift(<5.9)
extension Data: @unchecked Sendable {}
#endif

extension Data: KafkaContiguousBytes {}
