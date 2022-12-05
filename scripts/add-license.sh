#!/usr/bin/env bash
# Add a license header for all source files.
set -eu

for i in $(find . -type f -not -path '*/\.*' | grep '\.go$'); do
    matches=$(sed -n "1{/Copyright [0-9]\{4\} Tatris Project Authors. Licensed under Apache-2.0./p;};q;" $i)
    if [ -z "${matches}" ]; then
        echo "License header is missing from $i, try to add one"
        year=$(date +'%Y')
        sed -i "1i// Copyright $year Tatris Project Authors. Licensed under Apache-2.0.\n" $i
    fi
done
