#!/usr/bin/env bash
set -e


function fixAuth() {
  local dir=$1
  if [ ! -e "$dir" ]; then
    return
  fi
  dirOwner=`stat -c '%U' $dir`
  currentUser=`id -nu`

  if [ "$dirOwner" != "$currentUser" ]; then
      sudo chown $currentUser:$currentUser -R $dir
  fi
}

fixAuth /home/tatris/logs

exec sudo -E /usr/bin/supervisord -n
