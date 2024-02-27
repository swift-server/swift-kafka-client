#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the swift-kafka-client open source project
##
## Copyright (c) YEARS Apple Inc. and the swift-kafka-client project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##

cd Benchmarks
swift package --disable-sandbox benchmark baseline update PR --no-progress
git checkout main
swift package --disable-sandbox benchmark baseline update main --no-progress

swift package benchmark baseline check main PR
BENCHMARK_RESULT=$?

echo "Retcode is $BENCHMARK_RESULT"

if [ $BENCHMARK_RESULT -eq 0 ]; then
    echo "Benchmark results are the same as for main"
fi

if [ $BENCHMARK_RESULT -eq 4 ]; then
    echo "Benchmark results are better as for main"
fi

if [ $BENCHMARK_RESULT -eq 1 ]; then
    echo "Benchmark failed"
    exit 1
fi

if [ $BENCHMARK_RESULT -eq 2 ]; then
    echo "Benchmark results are worse than main"
    exit 1
fi
