# This source file is part of the SwiftConfiguration open source project
#
# Copyright (c) 2026 Apple Inc. and the SwiftConfiguration project authors
# Licensed under Apache License v2.0
#
# See LICENSE.txt for license information
# See CONTRIBUTORS.txt for the list of SwiftConfiguration project authors
#
# SPDX-License-Identifier: Apache-2.0

SWIFT_BUILD_ARGS := -Xswiftc -warnings-as-errors --explicit-target-dependency-import-check error
SWIFT_TEST_ARGS := $(SWIFT_BUILD_ARGS)

.PHONY: gyb
gyb:
	find Sources -name '*.swift.gyb' | while read -r file; do \
		./dev/gyb.py --line-directive '' -o "$${file%.gyb}" "$$file"; \
		swift format -i "$${file%.gyb}"; \
		done

.PHONY: build
build: gyb
	swift build $(SWIFT_BUILD_ARGS)

.PHONY: test
test:
	swift test $(SWIFT_TEST_ARGS)
