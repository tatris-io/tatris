#!/usr/bin/env bash
# Check all source files, make sure that they have a license header.
set -eu

for i in $(git ls-files --exclude-standard | grep "\.go$"); do
    # first line -> match -> print line -> quit
    matches=$(sed -n "1{/Copyright [0-9]\{4\} Tatris Project Authors. Licensed under Apache-2.0./p;};q;" $i)
    if [ -z "${matches}" ]; then
        echo "License header is missing from $i."
        echo "\tAdvice: exec ./scripts/add-license.sh"
        exit 1
    fi
done
