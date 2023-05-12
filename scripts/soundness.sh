#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the swift-kafka-gsoc open source project
##
## Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##

set -eu
here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function replace_acceptable_years() {
    # this needs to replace all acceptable forms with 'YEARS'
    sed -e 's/20[12][789012]-20[12][89012]/YEARS/' -e 's/20[12][89012]/YEARS/'
}

printf "=> Checking for unacceptable language... "
# This greps for unacceptable terminology. The square bracket[s] are so that
# "git grep" doesn't find the lines that greps :).
unacceptable_terms=(
    -e blacklis[t]
    -e whitelis[t]
    -e slav[e]
    -e sanit[y]
)

# We have to exclude the code of conduct because it gives examples of unacceptable language.
# We have to exclude *Config.swift files as they need to map to Kafka terminology
# which is considered unacceptable by us.
exclude_files=(
    CODE_OF_CONDUCT.md
    *Config.swift
)
for word in "${exclude_files[@]}"; do
    exclude_files+=(":(exclude)$word")
done
exclude_files_str=$(printf " %s" "${exclude_files[@]}")

if git grep --color=never -i "${unacceptable_terms[@]}" -- . $exclude_files_str > /dev/null; then
    printf "\033[0;31mUnacceptable language found.\033[0m\n"
    git grep -i "${unacceptable_terms[@]}" -- . $exclude_files_str
    exit 1
fi
printf "\033[0;32mokay.\033[0m\n"

printf "=> Checking format... "
FIRST_OUT="$(git status --porcelain)"
swiftformat . > /dev/null 2>&1
SECOND_OUT="$(git status --porcelain)"
if [[ "$FIRST_OUT" != "$SECOND_OUT" ]]; then
  printf "\033[0;31mformatting issues!\033[0m\n"
  git --no-pager diff
  exit 1
else
  printf "\033[0;32mokay.\033[0m\n"
fi

printf "=> Checking license headers... "
tmp=$(mktemp /tmp/.swift-kafka-gsoc-soundness_XXXXXX)

for language in swift-or-c bash dtrace python; do
  declare -a matching_files
  declare -a exceptions
  expections=( )
  matching_files=( -name '*' )
  case "$language" in
      swift-or-c)
        # we don't add our own headers to the librdkafka submodule
        exceptions=( -path '*Sources/Crdkafka/*' -o -name Package.swift )
        matching_files=( -name '*.swift' -o -name '*.c' -o -name '*.h' )
        cat > "$tmp" <<"EOF"
//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) YEARS Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
EOF
        ;;
      bash)
        matching_files=( -name '*.sh' )
        cat > "$tmp" <<"EOF"
#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the swift-kafka-gsoc open source project
##
## Copyright (c) YEARS Apple Inc. and the swift-kafka-gsoc project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##
EOF
      ;;
      python)
        matching_files=( -name '*.py' )
        cat > "$tmp" <<"EOF"
#!/usr/bin/env python3
##===----------------------------------------------------------------------===##
##
## This source file is part of the swift-kafka-gsoc open source project
##
## Copyright (c) YEARS Apple Inc. and the swift-kafka-gsoc project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##
EOF
      ;;
      dtrace)
        matching_files=( -name '*.d' )
        cat > "$tmp" <<"EOF"
#!/usr/sbin/dtrace -q -s
/*===----------------------------------------------------------------------===*
 *
 *  This source file is part of the swift-kafka-gsoc open source project
 *
 *  Copyright (c) YEARS Apple Inc. and the swift-kafka-gsoc project authors
 *  Licensed under Apache License v2.0
 *
 *  See LICENSE.txt for license information
 *  See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *===----------------------------------------------------------------------===*/
EOF
      ;;
    *)
      echo >&2 "ERROR: unknown language '$language'"
      ;;
  esac

  expected_lines=$(cat "$tmp" | wc -l)
  expected_sha=$(cat "$tmp" | shasum)

  (
    cd "$here/.."
    {
        find . \
            \( \! -path './.build/*' -a \
            \( "${matching_files[@]}" \) -a \
            \( \! \( "${exceptions[@]}" \) \) \)

        if [[ "$language" = bash ]]; then
            # add everything with a shell shebang too
            git grep --full-name -l '#!/bin/bash'
            git grep --full-name -l '#!/bin/sh'
        fi
    } | while read line; do
      if [[ "$(cat "$line" | replace_acceptable_years | head -n $expected_lines | shasum)" != "$expected_sha" ]]; then
        printf "\033[0;31mmissing headers in file '$line'!\033[0m\n"
        diff -u <(cat "$line" | replace_acceptable_years | head -n $expected_lines) "$tmp"
        exit 1
      fi
    done
    printf "\033[0;32mokay.\033[0m\n"
  )
done

rm "$tmp"
