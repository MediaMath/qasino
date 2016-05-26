#!/bin/bash
set -e

if [ -z "$1" ]; then

  if [ -z $QASINO_HOST ]; then
    echo "Please specify a QASINO_HOST."
    exit 1
  fi

  exec python qasino_sqlclient.py -H $QASINO_HOST
fi

exec "$@"
