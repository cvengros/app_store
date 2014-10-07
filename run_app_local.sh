#!/bin/sh

function die() {
  echo "$1" >&2
  exit 1
}

. config/run_credentials.sh

function get_cmd(){
  echo "bundle exec gooddata -lv -U $GD_USER -P $1 -p $PROJECT  run_ruby -d apps/$APP/ --name 'ha' --params config/$APP/runtime.json --credentials config/$APP/credentials.json"
}


APP="$1"
PROJECT=${2:-$GD_PROJECT}

if [ ! "$APP" ] ; then
  die "Usage: $0 <app> [<pid>] if pid not given, taken from run_credentials"
fi


CMD=`get_cmd "$GD_PASSWORD"`
CMD_PRINT=`get_cmd "******"`

echo "$CMD_PRINT"
#echo "$CMD"
eval "$CMD"
