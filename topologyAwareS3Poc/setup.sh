#!/bin/bash
set -e
chmod 755 *.sh

if [[ `uname -s` == "Linux" ]] ; then
    wget https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    wget https://github.com/elek/flekszible/releases/download/v1.8.1/flekszible_1.8.1_Linux_x86_64.tar.gz
    tar -xvf flekszible_1.8.1_Linux_x86_64.tar.gz
fi

CWD=`pwd`
export PATH=$PATH:$CWD
cd ..
echo building poc. build output at: `pwd`/topologyAwareS3Poc/pocBuild.txt
mvn clean install -DskipShade -DskipTests >& topologyAwareS3Poc/pocBuild.txt
echo starting kubernetes
pocStart.sh



