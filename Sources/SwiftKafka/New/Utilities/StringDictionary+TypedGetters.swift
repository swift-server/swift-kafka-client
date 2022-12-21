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

extension Dictionary where Key == String, Value == String {

    // TODO: docc
    func getInt(_ key: String) -> Int? {
        guard let value = self[key] else {
            return nil
        }
        return Int(value)
    }

    func getUInt(_ key: String) -> UInt? {
        guard let value = self[key] else {
            return nil
        }
        return UInt(value)
    }

    func getBool(_ key: String) -> Bool? {
        guard let value = self[key] else {
            return nil
        }
        return Bool(value)
    }
}
