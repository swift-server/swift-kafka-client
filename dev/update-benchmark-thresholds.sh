#!/bin/bash

set -eu
set -o pipefail

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
target_repo=${2-"$here/.."}

for f in 57 58 59 510 -nightly; do
    echo "swift$f"

    docker_file=$(if [[ "$f" == "-nightly" ]]; then f=main; fi && ls "$target_repo/docker/docker-compose."*"$f"*".yaml")

    docker-compose -f docker/docker-compose.yaml -f $docker_file run update-benchmark-baseline
done
