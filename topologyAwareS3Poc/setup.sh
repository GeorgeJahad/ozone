#!/bin/bash
set -e
mkdir topologyAwareS3Poc
cd topologyAwareS3Poc
git clone https://github.com/GeorgeJahad/ozone.git --branch topologyAwareS3Poc
cd ozone/topologyAwareS3Poc
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
#pocStart.sh
#pocLog.sh &


