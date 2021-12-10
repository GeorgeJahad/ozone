#!/bin/bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source $SCRIPT_DIR/vars.sh
IP_ADDR=`kubectl get pods -o wide | grep ${nodeMap[$1]} | awk '{print $6}'`
mc rm $1/s3-registry-bucket/$IP_ADDR