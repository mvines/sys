#!/usr/bin/env bash

set -e

case "$(uname)" in
Linux)
  TARGET=x86_64-unknown-linux-gnu
  ;;
Darwin)
  TARGET=x86_64-apple-darwin
  ;;
*)
  echo "machine architecture is currently unsupported"
  exit 1
  ;;
esac

if [[ -z $1 || $1 = master ]]; then
  RELEASE_BINARY=https://github.com/mvines/sys/raw/master-bin/sys-$TARGET
elif [[ -n $1 ]]; then
  RELEASE_BINARY=https://github.com/mvines/sys/releases/download/$1/sys-$TARGET
else
  RELEASE_BINARY=https://github.com/mvines/sys/releases/latest/download/sys-$TARGET
fi

set -x
curl -sSfL $RELEASE_BINARY -o sys
chmod +x sys
ls -l sys
./sys --version
