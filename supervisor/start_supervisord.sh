#!/usr/bin/env bash
set -e

if ! ps aux | grep /usr/bin/supervisord | grep -v grep >/dev/null 2>&1; then
  sudo -E supervisord
fi
