#!/usr/bin/env bash
# Check all source files, make sure that they have a license header.
set -eu

sed_bin=sed

if [ `uname` = "Darwin" ]; then
  sed_bin="gsed"
fi

if ! which $sed_bin >/dev/null 2>/dev/null; then
  echo miss $sed_bin
  exit 1
fi

project_root=`dirname $0 | xargs -I{} realpath {}/..`

for i in $(find $project_root -type f -not -path '*/\.*' | grep '\.go$' | grep -v 'dependencies'); do
    # first line -> match -> print line -> quit
    matches=$($sed_bin -n "1{/Copyright [0-9]\{4\} Tatris Project Authors. Licensed under Apache-2.0./p;};q;" $i)
    if [ -z "${matches}" ]; then
        echo "License header is missing from $i."
        echo "\tAdvice: exec ./scripts/add-license.sh"
        exit 1
    fi
done
