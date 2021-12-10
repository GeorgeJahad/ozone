#!/bin/bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source $SCRIPT_DIR/vars.sh
mc cp  $1/$DUMMY_BUCKET/$DUMMY_FILE /tmp/$DUMMY_FILE >& /dev/null



