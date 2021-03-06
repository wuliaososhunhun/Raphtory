#!/bin/bash
# Run me as env-setter.sh (partitionManager|router|seedNode|clusterUp|updater)
echo "Starting up..."

export HOST_IP=$(ip addr show eth0 | grep inet[^6] | sed 's/.*inet \(.*\)\/[0-9]* \(.* \)*scope.*/\1/')
export HOSTNAME=$(hostname)

echo "/////  ENV SET //////"
echo "HOST_IP      = $HOST_IP"
echo "HOSTNAME     = $HOSTNAME"
echo "/////////////////////"

cd /opt/docker/bin
go $1
