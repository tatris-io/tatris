#!/usr/bin/env bash
# Add a license header for all source files.
set -eu

sed_bin=sed

if [ `uname` = "Darwin" ]; then
  # gsed is preferred in Darwin because it is more canonical.
  sed_bin="gsed"
fi

if ! which $sed_bin >/dev/null 2>/dev/null; then
  echo miss bin $sed_bin
  exit 1
fi

project_root=`dirname $0 | xargs -I{} realpath {}/..`

for i in $(find $project_root -type f -not -path '*/\.*' | grep '\.go$'); do
    matches=$($sed_bin -n "1{/Copyright [0-9]\{4\} Tatris Project Authors. Licensed under Apache-2.0./p;};q;" $i)
    if [ -z "${matches}" ]; then
        echo "License header is missing from $i, try to add one"
        year=$(date +'%Y')
        $sed_bin -i "1i// Copyright $year Tatris Project Authors. Licensed under Apache-2.0.\n" $i
    fi
done
