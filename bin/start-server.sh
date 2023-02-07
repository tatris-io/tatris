#!/usr/bin/env bash
set -e

# Absolute path to this script
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in
SCRIPT_PATH=$(dirname "$SCRIPT")

# Absolute path to the real binary
BINARY=$SCRIPT_PATH/tatris-server

# Absolute path to the home of Tatris
TATRIS_HOME="$(dirname "$SCRIPT_PATH")"

# Default config files
DEFAULT_CONF_PATH=$TATRIS_HOME/conf
DEFAULT_CONF_SERVER=$TATRIS_HOME/conf/server-conf.yml
DEFAULT_CONF_LOGGING=$TATRIS_HOME/conf/log-conf.yml

# Override the default conf location with env variables
if [[ -z "${TATRIS_PATH_SERVER_CONF}" ]]; then
  _PATH_SERVER_CONF=$DEFAULT_CONF_SERVER
else
  _PATH_SERVER_CONF="${TATRIS_PATH_SERVER_CONF}"
fi
if [[ -z "${TATRIS_PATH_LOGGING_CONF}" ]]; then
  _PATH_LOGGING_CONF=$DEFAULT_CONF_LOGGING
else
  _PATH_LOGGING_CONF="${TATRIS_PATH_LOGGING_CONF}"
fi

# Check the existence of configuration files
BOOTSTRAP_ARGS=""
if [ -e ${_PATH_SERVER_CONF} ]
then
    echo "Using server conf, path:" ${_PATH_SERVER_CONF}
    BOOTSTRAP_ARGS=$BOOTSTRAP_ARGS" --conf.server=${_PATH_SERVER_CONF}"
else
    echo "No such server conf, path:" ${_PATH_SERVER_CONF}
fi
if [ -e ${_PATH_LOGGING_CONF} ]
then
    echo "Using logging conf, path:" ${_PATH_LOGGING_CONF}
    BOOTSTRAP_ARGS=$BOOTSTRAP_ARGS" --conf.logging=${_PATH_LOGGING_CONF}"
else
    echo "No such logging conf, path:" ${_PATH_LOGGING_CONF}
fi

echo "Starting Tatris Server: " $BINARY $BOOTSTRAP_ARGS

$BINARY $BOOTSTRAP_ARGS
